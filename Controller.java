import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Controller {
    private static Integer cport;
    private static Integer timeout;
    private static Integer replicationFactor;
    private static Integer rebalancePeriod;

    //Connected DStores
    private static ConcurrentHashMap<Integer, DstoreAndFiles> dStores = new ConcurrentHashMap<>();

    //The Index
    private static Index index = new Index();

    //List Operation
    private static ConcurrentHashMap<Integer, Set<String>> dStoreListFiles = new ConcurrentHashMap<>();
    private static CountDownLatch awaitingList;

    //Countdown Latch For Acks
    private static ConcurrentHashMap<String, CountDownLatch> awaitingStore = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, CountDownLatch> awaitingRemove = new ConcurrentHashMap<>();
    private static CountDownLatch awaitingRebalance;

    //Locks
    private static ReentrantReadWriteLock rebalanceLock = new ReentrantReadWriteLock();

    //Logger
    private static Logger logger = Logger.getLogger(Controller.class);

    public static void main(String[] args) {
        try {
            cport = Integer.parseInt(args[0]);
            replicationFactor = Integer.parseInt(args[1]); 
            timeout = Integer.parseInt(args[2]); //ms
            rebalancePeriod = Integer.parseInt(args[3]); //s
        } catch (Exception e) {
            logger.err("Malformed arguemnts; java Controller cport R timeout rebalance_period");
            return;
        }
        if (!(replicationFactor > 0 && timeout > 0 && rebalancePeriod > 0)) {
            logger.err("Arguments must be > 0");
            return;
        }

        logger.info("Accepting Connections");
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.scheduleWithFixedDelay(new Rebalance(), rebalancePeriod, rebalancePeriod, TimeUnit.SECONDS);
        acceptConnections();
    }

    public static void acceptConnections() {
        try (ServerSocket serverSocket = new ServerSocket(cport)) {
            for (;;) {
                try {
                    Socket client = serverSocket.accept();
                    new UnknownHandler(client).start(); 
                } catch (IOException e) { logger.err("Error Creating Client"); }
            }
        } catch (IOException e) {
            logger.err("Error Accepting Connections - Clients can no longer connect");
            e.printStackTrace();
        }
    }

    public static class Rebalance implements Runnable {
        @Override
        public void run() {
            if (dStores.size() < replicationFactor) {
                logger.err("Cannot Rebalance - NOT ENOUGH DSTORES");
                return;
            }
            rebalanceLock.writeLock().lock();
            logger.info("Attemping Rebalance");
            //ATTEMPT TO GET LIST FROM EACH DSTORE
            try {
                awaitingList = new CountDownLatch(dStores.size());
                dStoreListFiles.clear();

                dStores.values().forEach(dStore -> {
                    dStore.getDstore().sendMessage(Protocol.LIST);
                });

                try {
                    if (!awaitingList.await(timeout, TimeUnit.MILLISECONDS)) return;
                } catch (Exception e) { return; } finally {
                    awaitingList = null;
                }

                //COMPARE FILES FROM DSTORE TO FILES LIST THAT CONTROLLER HAS
                HashMap<Integer, ArrayList<String>> extraFiles = new HashMap<>(); // Extra files the dstores have (not in controller list)
                dStoreListFiles.entrySet().stream().forEach(e -> extraFiles.put(e.getKey(), 
                    new ArrayList<>(e.getValue().stream().filter(f -> dStores.get(e.getKey()).getFiles().contains(f)).collect(Collectors.toList()))));
                
                HashMap<Integer, ArrayList<String>> missingFiles = new HashMap<>(); // Files which the dstores dont have
                dStores.entrySet().stream().forEach(e -> missingFiles.put(e.getKey(), 
                    new ArrayList<>(e.getValue().getFiles().stream().filter(f -> !dStoreListFiles.get(e.getKey()).contains(f)).collect(Collectors.toList()))));         

                missingFiles.entrySet().forEach(e -> { // Remove missing files
                    dStores.get(e.getKey()).getFiles().removeAll(e.getValue());
                });

                index.keySet().stream().forEach(f -> { // Remove files which no dstore has
                    if (!dStores.entrySet().stream().anyMatch(e -> e.getValue().getFiles().contains(f))) index.remove(f);
                });

                // Attempt to fix remove in progress files
                var filesRemovingInProgress = index.entrySet().stream()
                    .filter(e -> Objects.equals(e.getValue().getStatus(), status.remove_in_progress))
                    .map(Map.Entry::getKey).collect(Collectors.toList());

                filesRemovingInProgress.stream().forEach(f -> new fixRemoveInProgress(f).start());

                //Rebalance now
                HashMap<Integer, Set<Entry<Integer, String>>> filesToSend = new HashMap<>();
                HashMap<Integer, ArrayList<String>> filesToRemove = new HashMap<>();
                HashMap<Integer, Set<String>> dStoresCopy = new HashMap<>();
                dStores.entrySet().stream().forEach(e -> dStoresCopy.put(e.getKey(), 
                    e.getValue().getFiles().stream().filter(f -> index.getStatus(f).equals(status.store_complete)).collect(Collectors.toSet())));
                dStoresCopy.keySet().stream().forEach(e -> {
                    filesToSend.put(e, new HashSet<>()); filesToRemove.put(e, new ArrayList<>());
                });

                AtomicBoolean noChanges = new AtomicBoolean(true);

                //ENSURE FILE IS REPLICATED R TIMES
                index.keySet().forEach(filename -> {
                    Integer copies = dStoresCopy.entrySet().stream()
                        .filter(e -> e.getValue().contains(filename))
                        .collect(Collectors.toList()).size();
                    while (!copies.equals(replicationFactor)) {
                        if (copies > replicationFactor) {
                            Integer dStoreToRemove = 
                                (dStoresCopy.entrySet().stream().filter(e -> e.getValue().contains(filename))
                                .sorted(Comparator.comparing(e -> dStoresCopy.get(e.getKey()).size(), Comparator.reverseOrder()))
                                .collect(Collectors.toList())).get(0).getKey();
                            dStoresCopy.get(dStoreToRemove).remove(filename);
                            filesToRemove.get(dStoreToRemove).add(filename);
                        } else if (copies < replicationFactor) {
                            Integer dStoreToReceive = 
                                (dStoresCopy.entrySet().stream().filter(e -> !e.getValue().contains(filename))
                                .sorted(Comparator.comparingInt(e -> dStoresCopy.get(e.getKey()).size()))
                                .collect(Collectors.toList())).get(0).getKey();
                            Integer dStoreToGet = 
                                (dStoresCopy.entrySet().stream().filter(e -> e.getValue().contains(filename))
                                .sorted(Comparator.comparing(e -> dStoresCopy.get(e.getKey()).size()))
                                .collect(Collectors.toList())).get(0).getKey();
                            dStoresCopy.get(dStoreToReceive).add(filename);
                            filesToSend.get(dStoreToGet).add(Map.entry(dStoreToReceive, filename));
                        }
                        copies = dStoresCopy.entrySet().stream()
                            .filter(e -> e.getValue().contains(filename))
                            .collect(Collectors.toList()).size();
                            noChanges.set(false);
                    } 
                });

                //ENSURE DSTORES HAVE FILES EVENLY SPREAD
                Boolean balanced = dStoresCopy.entrySet().stream()
                    .map(e -> e.getValue().stream().filter(f -> index.isStoreComplete(f)).collect(Collectors.toList()).size())
                    .allMatch(e -> (e >= Math.floor((replicationFactor * index.sizeCompleted())/(float)dStores.size())) 
                        || (e >= Math.ceil((replicationFactor * index.sizeCompleted())/(float)dStores.size())));

                if (balanced && noChanges.get()) {
                    logger.info("Files are already balanced - No messages to send"); return;
                }

                while (!balanced) {
                    ArrayList<Integer> dStoresSorted = new ArrayList<>(dStoresCopy.keySet().stream()
                        .sorted(Comparator.comparingInt(e -> dStoresCopy.get(e).size()))
                        .collect(Collectors.toList()));
                    Integer dStoreToReceive = dStoresSorted.get(0);
                    Integer dStoreToSend = dStoresSorted.get(dStoresSorted.size() - 1);
                    String fileToMove = dStoresCopy.get(dStoreToSend).stream()
                        .filter(f -> !dStoresCopy.get(dStoreToReceive).contains(f))
                        .collect(Collectors.toList()).get(0);

                    filesToSend.get(dStoreToSend).add(Map.entry(dStoreToReceive, fileToMove));
                    filesToRemove.get(dStoreToSend).add(fileToMove);
                    dStoresCopy.get(dStoreToSend).remove(fileToMove);
                    dStoresCopy.get(dStoreToReceive).add(fileToMove);

                    balanced = dStoresCopy.entrySet().stream()
                        .map(e -> e.getValue().stream().filter(f -> index.isStoreComplete(f)).collect(Collectors.toList()).size())
                        .allMatch(e -> (e >= Math.floor((replicationFactor * index.sizeCompleted())/(float)dStoresCopy.size())) 
                            || (e >= Math.ceil((replicationFactor * index.sizeCompleted())/(float)dStoresCopy.size())));
                }

                // SEND MESSAGES :)
                if (!(dStoresCopy.size() == dStores.size())) return; // Ensure no dStore has disconnected

                awaitingRebalance = new CountDownLatch(dStores.size());

                dStores.entrySet().forEach(dStore -> {
                    Map<String, List<Integer>> filesToPorts = filesToSend.get(dStore.getKey()).stream().collect(Collectors.groupingBy(
                                Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

                    String filesToPortsString;
                    if (filesToPorts.size() == 0) filesToPortsString = "0"; 
                    else filesToPortsString = filesToPorts.size() + " " + filesToPorts.entrySet()
                            .stream().map(e -> e.getKey() + " " + e.getValue().size() + " " + 
                            String.join(" ", e.getValue().stream().map(Object::toString)
                            .collect(Collectors.toUnmodifiableList()))).collect(Collectors.joining(" "));

                    String message = Protocol.REBALANCE + " " + filesToPortsString + " " + 
                        filesToRemove.get(dStore.getKey()).size() + " " + String.join(" ", filesToRemove.get(dStore.getKey()));
                    
                    dStore.getValue().getDstore().sendMessage(message);
                });
                
                if (awaitingRebalance.await(timeout, TimeUnit.MILLISECONDS)) {
                    dStores.entrySet().forEach(e -> {
                        e.getValue().setFiles(dStoresCopy.get(e.getKey()));
                    });
                    logger.info("Rebalance Successful");
                } else {
                    logger.err("Rebalance Failed");
                }

            } catch (Exception e) {
                logger.err("Error Rebalancing");
                e.printStackTrace();
            } finally {
                logger.info("Rebalance Complete");
                dStoreListFiles.clear();
                awaitingRebalance = null;
                rebalanceLock.writeLock().unlock();
            }
        }

        public class fixRemoveInProgress extends Thread {
            private String filename;

            public fixRemoveInProgress(String filename) {
                this.filename = filename;
            }

            public void run() {
                ArrayList<Integer> dStoresWithFile = new ArrayList<>(dStores.entrySet().stream() // Get dStores
                    .filter(e -> e.getValue().getFiles().contains(filename))
                    .map(Map.Entry::getKey).collect(Collectors.toList()));
                CountDownLatch latch = new CountDownLatch(dStoresWithFile.size());
                awaitingRemove.put(filename, latch);
                dStoresWithFile.forEach(dStorePort -> { // SEND REMOVE MESSAGES
                    dStores.get(dStorePort).getDstore().sendMessage(Protocol.REMOVE + " " + filename);
                });
                try {
                    if (latch.await(timeout, TimeUnit.MILLISECONDS)) { // Wait for acks, with timeout
                        index.remove(filename);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    awaitingRemove.remove(filename);
                }
            }
        }
    }
    
    private static class ClientHandler extends Thread {
        private final Socket socket;

        //Reader and Writers
        private final PrintWriter out;
        private final BufferedReader in;

        private ArrayList<Integer> attemptedLoad = new ArrayList<>();

        //Logger
        private static Logger logger = Logger.getLogger(ClientHandler.class);

        public ClientHandler(Socket socket, String message) throws IOException {
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
           
            logger.info("New Client Created");
            handleMessage(message);
        }
    
        public void run() {
            try (socket; out; in){
                while(true) {
                    String message = in.readLine();
                    if (message == null) {
                        return;
                    }
                    handleMessage(message);
                }
            } catch (SocketException e) {
                // Expected error when client disconnects
            } catch (Exception e) {
                logger.err("Error in client: " + e);
            }
        }

        public void handleMessage(String message) { 
            logger.info("Message received from client, "+ socket.getPort() + ": " + message);
            try {
                var arguments = message.split(" ");

                switch (getOperation(message)) {
                    case Protocol.LIST -> list();
                    case Protocol.STORE -> {
                        var filename = arguments[1];
                        var filesize = Integer.parseInt(arguments[2]);
                        store(filename, filesize); 
                    }
                    case Protocol.LOAD -> {
                        var filename = arguments[1];
                        if (!attemptedLoad.isEmpty()) attemptedLoad.clear();
                        load(filename);
                    }
                    case Protocol.RELOAD -> {
                        var filename = arguments[1];
                        load(filename);
                    }
                    case Protocol.REMOVE -> {
                        var filename = arguments[1];
                        remove(filename);
                    }
                    default -> logger.err("Malformed Message from client");
                }
            } catch (Exception e) {
                logger.err("Error in handling message");
                e.printStackTrace();
            }
            
        }

        private void list() {
            if (dStores.size() < replicationFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES);
                logger.err("NOT ENOUGH DSTORES");
                return;
            }

            String files = String.join(" ", index.entrySet() // Only list the stored files
                    .stream()
                    .filter(e -> Objects.equals(e.getValue().getStatus(), status.store_complete))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet()));
            out.println(Protocol.LIST + " " + files);
        }

        private void store(String filename, Integer filesize) {       
            if (dStores.size() < replicationFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES);
                logger.err("NOT ENOUGH DSTORES");
                return;
            }

            if (index.containsFile(filename)) {
                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS);
                logger.err("FILE ALREADY EXISTS");
                return;
            }

            rebalanceLock.readLock().lock();
            index.put(filename, status.store_in_progress, filesize); // Add status to index, start processing

            ArrayList<Integer> dStoresToUse = 
                    new ArrayList<>(dStores.keySet().stream()
                    .sorted(Comparator.comparingInt(e -> dStores.get(e).getFiles().size()))
                    .limit(replicationFactor).collect(Collectors.toList()));

            String toStore = 
                String.join(" ",  dStoresToUse.stream().map(String::valueOf).collect(Collectors.toList()));
            
            //Put ack queue in array
            CountDownLatch latch = new CountDownLatch(dStoresToUse.size());
            awaitingStore.put(filename, latch);

            logger.info("Storing to: " + toStore);
            out.println(Protocol.STORE_TO + " " + toStore);

            //Waiting for store acks (with latch)
            try {
                if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    logger.info("File: '" + filename + "' stored");
                    out.println(Protocol.STORE_COMPLETE);
                    index.setStatus(filename, status.store_complete);
                    dStoresToUse.forEach(e -> dStores.get(e).getFiles().add(filename));
                } else {
                    logger.err("Error storing file");
                    index.remove(filename);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                awaitingStore.remove(filename);
                rebalanceLock.readLock().unlock();
            }
        }

        private void load(String filename) { 
            if (dStores.size() < replicationFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES);
                logger.err("NOT ENOUGH DSTORES");
                return;
            }

            if (!index.getStatus(filename).equals(status.store_complete)) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST);
                logger.err("ERROR FILE DOES NOT EXIST");
                return;
            } 
            
            List<Integer> dStoresWithFile = dStores.entrySet().stream()
                                                    .filter(e -> e.getValue().getFiles().contains(filename))
                                                    .map(Map.Entry::getKey)
                                                    .collect(Collectors.toList());

            dStoresWithFile.removeAll(attemptedLoad);
            if (dStoresWithFile.isEmpty()) {
                out.println(Protocol.ERROR_LOAD);
                logger.err("ERROR LOADING FROM DSTORES");
                return;
            } 
            Integer dStorePort = dStoresWithFile.get(0);
            out.println(Protocol.LOAD_FROM + " " + dStorePort + " " + index.getSize(filename)); 
            logger.info("Attempting to load from dStore with port: " + dStorePort);
            attemptedLoad.add(dStorePort);
        }

        private void remove(String filename) {
            if (dStores.size() < replicationFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES);
                logger.err("NOT ENOUGH DSTORES");
                return;
            }

            if (!index.getStatus(filename).equals(status.store_complete)) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST);
                logger.err("ERROR FILE DOES NOT EXIST");
                return;
            }
            
            rebalanceLock.readLock().lock();
            index.setStatus(filename, status.remove_in_progress);  // Add status to index, start processing
            
            ArrayList<Integer> dStoresWithFile = new ArrayList<>(dStores.entrySet().stream()
                                                .filter(e -> e.getValue().getFiles().contains(filename))
                                                .map(Map.Entry::getKey)
                                                .collect(Collectors.toList()));
            
            CountDownLatch latch = new CountDownLatch(dStoresWithFile.size());
            awaitingRemove.put(filename, latch);

            dStoresWithFile.forEach(port -> { // Send remove message to each Dstore
                dStores.get(port).getDstore().sendMessage(Protocol.REMOVE + " " + filename);
            });

            try {
                if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    out.println(Protocol.REMOVE_COMPLETE);

                    index.remove(filename);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                awaitingRemove.remove(filename);
                rebalanceLock.readLock().unlock();
            }
        }
    }

    private static class UnknownHandler extends Thread {
        private final Socket socket;

        //Logger
        private static Logger logger = Logger.getLogger(UnknownHandler.class);

        public UnknownHandler(Socket socket) {
            this.socket = socket;
            logger.info("New Connection; port " + socket.getPort());
        }

        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String message = in.readLine();
                if (message == null) {
                    throw new Exception("Error reading message");
                }

                switch (getOperation(message)) {
                    case Protocol.JOIN -> {
                        var arguments = message.split(" ");
                        Integer port = Integer.parseInt(arguments[1]);
                        new DstoreHandler(socket, port).start();
                        new Thread(new Rebalance()).start();
                    }
                    default -> new ClientHandler(socket, message).start();
                }
            } catch (Exception e) { 
                e.printStackTrace(); 
                logger.err("Unkown Connection Lost");
            } 
        }
    }

    private static class DstoreHandler extends Thread {
        private final Socket socket;

        //Reader and Writers
        private final PrintWriter out;
        private final BufferedReader in;
                
        private Integer port;

        //Logger
        private static Logger logger = Logger.getLogger(DstoreHandler.class);

        public DstoreHandler(Socket socket, Integer port) throws IOException {
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.port = port;
            
            dStores.put(port, new DstoreAndFiles(this));
            logger.info("New DStore Created; port " + socket.getPort() + ". (" + dStores.size() + "/" + replicationFactor + ") DStores connected");
        }

        public void sendMessage(String message) {
            out.println(message);
        }

        public void run() {
            try (socket; out; in){
                while(true) {
                    String message = in.readLine();
                    if (message == null) {
                        return;
                    }

                    handleMessage(message);
                }    
            } catch (SocketException e) {
                // Expected error when dstore disconnects
            } catch (Exception e) {
                logger.err("Error in Dstore: " + e);
            } finally {
                dStores.remove(port);
            }
        }

        public void handleMessage(String message) {
            logger.info("Message received from dStore, " + socket.getPort() + ": " + message);
            try {
                var arguments = message.split(" ");

                switch (getOperation(message)) {
                    case Protocol.STORE_ACK -> {
                        var filename = arguments[1];
                        awaitingStore.get(filename).countDown();
                    }
                    case Protocol.REMOVE_ACK, Protocol.ERROR_FILE_DOES_NOT_EXIST -> {
                        var filename = arguments[1];
                        dStores.get(port).getFiles().remove(filename); // Remove file from dstore file list
                        awaitingRemove.get(filename).countDown();
                    }
                    case Protocol.LIST -> {
                        if (awaitingList == null) return;
                        var files = new HashSet<>(Arrays.asList(arguments)); 
                        files.remove(Protocol.LIST);
                        dStoreListFiles.put(port, files);
                        awaitingList.countDown();
                    }   
                    case Protocol.REBALANCE_COMPLETE -> awaitingRebalance.countDown();
                    default -> logger.err("Malformed Message from dstore");
                }
            } catch (Exception e) {
                logger.err("Error in handling message");
            }
        }
    }

    private static String getOperation(String message) {
        return message.split(" ", 2)[0];
    }

    public static class DstoreAndFiles {
        private DstoreHandler dStore;
        private Set<String> files;

        public DstoreAndFiles (DstoreHandler dStore) {
            this.dStore = dStore;
            this.files = ConcurrentHashMap.newKeySet();
        }

        public DstoreHandler getDstore() {
            return dStore;
        }

        public Set<String> getFiles() {
            return files;
        }

        public void setFiles(Set<String> files) {
            this.files = files;
        }
    }
    
    public static class Index {
        private ConcurrentHashMap<String, FileInfo> index;

        public Index () {
            index = new ConcurrentHashMap<>();
        }

        public void put(String filename, status status, Integer filesize) {
            index.put(filename, new FileInfo(status, filesize));
        }

        public void remove(String filename) {
            index.remove(filename);
        }

        public void setStatus(String filename, status status) {
            index.get(filename).setStatus(status);
        }

        public Integer getSize(String filename) {
            return index.get(filename).getSize(); 
        }

        public status getStatus(String filename) {
            return index.getOrDefault(filename, new FileInfo(status.remove_complete, 0)).getStatus();
        }

        public Set<Entry<String, Controller.FileInfo>> entrySet() {
            return index.entrySet();
        }

        public Set<String> keySet() {
            return index.keySet();
        }

        public Boolean containsFile(String filename) {
            return index.containsKey(filename);
        }

        public Boolean isStoreComplete(String filename) {
            return index.getOrDefault(filename, new FileInfo(status.remove_complete, 0))
                .getStatus().equals(status.store_complete);
        }

        public Integer sizeCompleted() {
            return index.entrySet().stream()
                .filter(e -> e.getValue().getStatus().equals(status.store_complete))
                .collect(Collectors.toList()).size();
        }
    }

    public static class FileInfo {
        private status status;
        private Integer filesize;

        public FileInfo (status status, Integer filesize) {
            this.status = status;
            this.filesize = filesize;
        }

        public void setStatus(status status) {
            this.status = status;
        }

        public status getStatus() {
            return this.status;
        }

        public Integer getSize() {
            return this.filesize;
        }
    }

    enum status {
        store_in_progress,
        store_complete,
        remove_in_progress,
        remove_complete
    }
}
 