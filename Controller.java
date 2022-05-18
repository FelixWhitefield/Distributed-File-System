import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Controller {
    //Values 
    private static Integer cport, timeout, replicationFactor, rebalancePeriod;

    //Connected DStores
    private static final ConcurrentHashMap<Integer, DstoreAndFiles> dStores = new ConcurrentHashMap<>();

    //The Index
    private static final Index index = new Index();

    //List Operation
    private static final ConcurrentHashMap<Integer, Set<String>> dStoreListFiles = new ConcurrentHashMap<>();
    private static CountDownLatch awaitingList;

    //Countdown Latch For Acks
    private static final ConcurrentHashMap<String, CountDownLatch> awaitingStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, CountDownLatch> awaitingRemove = new ConcurrentHashMap<>();
    private static CountDownLatch awaitingRebalance;

    //Locks
    private static final ReentrantReadWriteLock rebalanceLock = new ReentrantReadWriteLock();
    private static final ReentrantLock storeStatusLock = new ReentrantLock();
    private static final ReentrantLock removeStatusLock = new ReentrantLock();

    //Rebalance Scheduler
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static ScheduledFuture<?> rebalanceTask;

    private static final ExecutorService taskPool = Executors.newCachedThreadPool();
    
    //Logger
    private static final Logger logger = Logger.getLogger(Controller.class);

    public static void main(String[] args) {
        try { // Parse inputs
            cport = Integer.parseInt(args[0]);
            replicationFactor = Integer.parseInt(args[1]); 
            timeout = Integer.parseInt(args[2]); //ms
            rebalancePeriod = Integer.parseInt(args[3]); //s
        } catch (Exception e) {
            logger.err("Malformed arguemnts; java Controller cport R timeout rebalance_period");
            return;
        }

        if (!(replicationFactor > 0 && timeout > 0 && rebalancePeriod > 0)) { // Check arguments
            logger.err("Arguments must be > 0");
            return;
        }

        rebalanceTask = scheduler.schedule(new Rebalance(), rebalancePeriod, TimeUnit.SECONDS); // Rebal task
        acceptConnections(); // Accept Client/Dstore connections
    }

    public static void acceptConnections() {
        logger.info("Accepting Connections");
        try (ServerSocket serverSocket = new ServerSocket(cport)) {
            for (;;) {
                try {
                    Socket socket = serverSocket.accept();
                    new Thread(new UnknownHandler(socket)).start(); 
                } catch (IOException e) { logger.err("Error Creating Client"); }
            }
        } catch (IOException e) {
            logger.err("Error Accepting Connections - Clients can no longer connect");
        }
    }

    public static class Rebalance implements Runnable {
        @Override
        public void run() {
            try {
            logger.info("Attemping Rebalance..");
            rebalanceLock.writeLock().lock();
            rebalanceTask.cancel(false); // If there is another rebalance scheduled, cancel it

            if (dStores.size() < replicationFactor) {
                logger.err("Cannot Rebalance - NOT ENOUGH DSTORES");
                return;
            }
            
            //ATTEMPT TO GET LIST FROM EACH DSTORE
            awaitingList = new CountDownLatch(dStores.size()); 
            dStoreListFiles.clear();

            dStores.values().forEach(dStore ->  dStore.getDstore().sendMessage(Protocol.LIST));
            try {
                awaitingList.await(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e) { } finally {
                awaitingList = null;
            }
            var aliveDstores = dStoreListFiles.keySet(); // The dstores which responded
            if (aliveDstores.size() < replicationFactor) {
                logger.err("Cannot Rebalance - NOT ENOUGH DSTORES RESPONDED TO LIST");
                return;
            }
            
            // Remove missing files from dStores List
            dStoreListFiles.keySet().stream().forEach(k -> dStores.get(k).getFiles()
                .removeIf(f -> !dStoreListFiles.get(k).contains(f)));

            index.keySet().stream().forEach(f -> { // Remove files which no dstore has
                if (!dStores.values().stream().anyMatch(e -> e.getFiles().contains(f))) index.remove(f);
            });

            // Attempt to fix remove in progress files
            index.keySet().stream().filter(k -> index.getStatus(k).equals(Status.remove_in_progress))
                .forEach(f -> taskPool.execute(new fixRemoveInProgress(f)));

            //Rebalance now
            HashMap<Integer, Set<Entry<Integer, String>>> filesToSend = new HashMap<>();
            HashMap<Integer, ArrayList<String>> filesToRemove = new HashMap<>();
            HashMap<Integer, Set<String>> dStoresCopy = new HashMap<>(); // Make copy to manipulate
            aliveDstores.stream().forEach(p -> dStoresCopy.put(p, dStores.get(p).getFiles().stream()
                .filter(f -> index.getStatus(f).equals(Status.store_complete)).collect(Collectors.toSet())));
        
            dStoresCopy.keySet().stream().forEach(k -> {
                filesToSend.put(k, new HashSet<>()); filesToRemove.put(k, new ArrayList<>());
            });

            var noChanges = true;
            //ENSURE FILE IS REPLICATED R TIMES
            for (var filename : index.keySet().stream().filter(f -> index.isStoreComplete(f)).collect(Collectors.toSet())) { // Check stored each file
                Callable<Integer> copies = () -> (int) dStoresCopy.entrySet().stream()
                                                .filter(e -> e.getValue().contains(filename)).count();

                while (!copies.call().equals(replicationFactor) && copies.call() > 0) { // If copies is not equal to R
                    if (copies.call() > replicationFactor) { // If there are more copies than R
                        Integer dStoreToRemove = 
                            (dStoresCopy.entrySet().stream().filter(e -> e.getValue().contains(filename))
                            .sorted(Comparator.comparing(e -> dStoresCopy.get(e.getKey()).size(), Comparator.reverseOrder()))
                            .collect(Collectors.toList())).get(0).getKey();
                        dStoresCopy.get(dStoreToRemove).remove(filename);
                        filesToRemove.get(dStoreToRemove).add(filename);
                    } else if (copies.call() < replicationFactor) { // If there are less copies than R
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
                    noChanges = false;
                } 
            }

            //ENSURE DSTORES HAVE FILES EVENLY SPREAD
            Callable<Boolean> balanced = () -> dStoresCopy.entrySet().stream()
                .map(e -> e.getValue().size())
                .allMatch(e -> (e >= Math.floor((replicationFactor * index.sizeCompleted())/(float)dStoresCopy.size())) 
                    || (e >= Math.ceil((replicationFactor * index.sizeCompleted())/(float)dStoresCopy.size()))); 

            if (balanced.call() && noChanges) {
                logger.info("Files are already balanced - No messages to send"); return;
            }

            while (!balanced.call()) {
                ArrayList<Integer> dStoresSorted = new ArrayList<>(dStoresCopy.keySet().stream()
                    .sorted(Comparator.comparingInt(e -> dStoresCopy.get(e).size()))
                    .collect(Collectors.toList()));
                Integer dStoreToReceive = dStoresSorted.get(0);
                Integer dStoreToSend = dStoresSorted.get(dStoresSorted.size() - 1);
                String fileToMove = dStoresCopy.get(dStoreToSend).stream()
                    .filter(f -> !dStoresCopy.get(dStoreToReceive).contains(f)).findFirst().orElse(null);

                filesToSend.get(dStoreToSend).add(Map.entry(dStoreToReceive, fileToMove));
                filesToRemove.get(dStoreToSend).add(fileToMove);
                dStoresCopy.get(dStoreToSend).remove(fileToMove);
                dStoresCopy.get(dStoreToReceive).add(fileToMove);
            }

            // SEND MESSAGES :)
            awaitingRebalance = new CountDownLatch(dStoresCopy.size());

            dStoresCopy.entrySet().stream().forEach(dStore -> {
                Map<String, List<Integer>> filesToPorts = filesToSend.get(dStore.getKey()).stream().collect(Collectors.groupingBy(
                            Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

                String filesToPortsString;
                filesToPortsString = (filesToPorts.size() == 0) ? "0" :
                        filesToPorts.size() + " " + filesToPorts.entrySet()
                        .stream().map(e -> e.getKey() + " " + e.getValue().size() + " " + 
                        String.join(" ", e.getValue().stream().map(Object::toString)
                        .collect(Collectors.toUnmodifiableList()))).collect(Collectors.joining(" "));

                String message = Protocol.REBALANCE + " " + filesToPortsString + " " + 
                    filesToRemove.get(dStore.getKey()).size() + " " + String.join(" ", filesToRemove.get(dStore.getKey()));
                
                dStores.get(dStore.getKey()).getDstore().sendMessage(message);
            });
            
            try {
                if (awaitingRebalance.await(timeout, TimeUnit.MILLISECONDS)) {
                    dStoresCopy.entrySet().forEach(d -> { // Update dStore Files
                        try { dStores.get(d.getKey()).setFiles(d.getValue()); } catch (Exception e) { }
                    });
                    logger.info("Rebalance Successful");
                } else {
                    logger.err("Rebalance Failed");
                }
            } catch (Exception e) {
                logger.err("Error waiting for rebalance_complete acks");
            }
            } catch (Exception e) { logger.err("Error Rebalancing"); e.printStackTrace(); } finally {
                dStoreListFiles.clear();
                awaitingRebalance = null;
                rebalanceTask = scheduler.schedule(new Rebalance(), rebalancePeriod, TimeUnit.SECONDS); // Set next rebalance
                logger.info("Rebalance Operation Complete");
                rebalanceLock.writeLock().unlock();
            }
        }

        public class fixRemoveInProgress implements Runnable {
            private final String filename;

            public fixRemoveInProgress(String filename) {
                this.filename = filename;
            }

            public void run() {
                ArrayList<Integer> dStoresWithFile = new ArrayList<>(dStores.entrySet().stream() // Get dStores
                    .filter(e -> e.getValue().getFiles().contains(filename))
                    .map(Map.Entry::getKey).collect(Collectors.toList()));
                CountDownLatch latch = new CountDownLatch(dStoresWithFile.size());
                awaitingRemove.put(filename, latch);
                dStoresWithFile.forEach(dStorePort -> // Send remove messages
                    dStores.get(dStorePort).getDstore().sendMessage(Protocol.REMOVE + " " + filename));
                try {
                    if (latch.await(timeout, TimeUnit.MILLISECONDS)) index.remove(filename);
                } catch (Exception e) {
                    logger.err("Error waiting for remove acks: '" + filename + "'");
                } finally {
                    awaitingRemove.remove(filename);
                }
            }
        }
    }
    
    private static class ClientHandler extends ConnectionHandler {
        private final Set<Integer> attemptedLoad = new HashSet<>();

        //Logger
        private static final Logger logger = Logger.getLogger(ClientHandler.class);

        public ClientHandler(Socket socket, String message) throws IOException {
            super(socket);
           
            logger.info("New Client Created");
            handleMessage(message);
        }
    
        public void run() {
            try (socket; out; in) {
                String message;
                while((message = in.readLine()) != null) handleMessage(message); 
            } catch (Exception e) { } finally {
                logger.info("Client Disconnected");
            }
        }

        public void handleMessage(String message) { 
            try {
                var arguments = message.split(" ");
                switch (getOperation(message)) {
                    case Protocol.LIST   -> list();
                    case Protocol.STORE  -> store(arguments[1], Integer.parseInt(arguments[2])); 
                    case Protocol.LOAD   -> load(arguments[1], false);
                    case Protocol.RELOAD -> load(arguments[1], true);
                    case Protocol.REMOVE -> remove(arguments[1]);
                    default -> logger.err("Malformed Message from client");
                }
            } catch (Exception e) {
                logger.err("Error handling client message; port: " + socket.getPort());
            }
        }

        private void list() {
            if (dStores.size() < replicationFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES);
                logger.err("LIST - NOT ENOUGH DSTORES");
                return;
            }
            logger.info("List operation started..");
            String files = String.join(" ", index.keySet() // Only list the stored files
                    .stream().filter(f -> index.isStoreComplete(f))
                    .collect(Collectors.toSet()));
            out.println((files.length() == 0) ? Protocol.LIST : Protocol.LIST + " " + files);
            logger.info("List operation complete");
        }

        private void store(String filename, Integer filesize) {       
            if (dStores.size() < replicationFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES);
                logger.err("STORE - NOT ENOUGH DSTORES");
                return;
            }
            storeStatusLock.lock();
            if (index.containsFile(filename)) {
                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS);
                logger.err("STORE - FILE ALREADY EXISTS");
                storeStatusLock.unlock();
                return;
            }
            logger.info("Store operation started for file: '" + filename + "'..");
            rebalanceLock.readLock().lock();
            index.put(filename, Status.store_in_progress, filesize); // Add status to index, start processing
            storeStatusLock.unlock();

            List<Integer> dStoresToUse =  dStores.keySet().stream()
                        .sorted(Comparator.comparingInt(e -> dStores.get(e).getFiles().size()))
                        .limit(replicationFactor).collect(Collectors.toList());

            String toStore = String.join(" ",  
                dStoresToUse.stream().map(String::valueOf).collect(Collectors.toList()));
            
            //Put ack queue in array
            CountDownLatch latch = new CountDownLatch(dStoresToUse.size());
            awaitingStore.put(filename, latch);

            logger.info("Storing to: " + toStore);
            out.println(Protocol.STORE_TO + " " + toStore);

            try { //Waiting for store acks (with latch)
                if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    dStoresToUse.forEach(e -> dStores.get(e).getFiles().add(filename));
                    index.setStatus(filename, Status.store_complete);
                    out.println(Protocol.STORE_COMPLETE);
                    logger.info("Store of file '" + filename + "' complete");
                } else {
                    logger.err("Store operation timed out for: '" + filename + "'");
                    index.remove(filename);
                }
            } catch (Exception e) {
                logger.err("Error completing store for: '" + filename + "'");
            } finally {
                awaitingStore.remove(filename);
                rebalanceLock.readLock().unlock();
            }
        }

        private void load(String filename, Boolean isReload) { 
            if (!isReload) attemptedLoad.clear(); // If not a reload, clear reload list
            if (dStores.size() < replicationFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES);
                logger.err("REMOVE - NOT ENOUGH DSTORES");
                return;
            }
            if (!index.getStatus(filename).equals(Status.store_complete)) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST);
                logger.err("REMOVE - ERROR FILE DOES NOT EXIST");
                return;
            } 
            logger.info("Load operation started for file: '" + filename + "'..");

            List<Integer> dStoresWithFile = dStores.entrySet().stream()
                                    .filter(e -> e.getValue().getFiles().contains(filename))
                                    .map(Map.Entry::getKey).collect(Collectors.toList());

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
                logger.err("REMOVE - NOT ENOUGH DSTORES");
                return;
            }
            removeStatusLock.lock();
            if (!index.getStatus(filename).equals(Status.store_complete)) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST);
                logger.err("REMOVE - ERROR FILE DOES NOT EXIST");
                removeStatusLock.unlock();
                return;
            }
            logger.info("Remove operation started for: '" + filename + "'..");
            rebalanceLock.readLock().lock();
            index.setStatus(filename, Status.remove_in_progress);  // Add status to index, start processing
            removeStatusLock.unlock();

            Set<Integer> dStoresWithFile = dStores.entrySet().stream()
                                                .filter(e -> e.getValue().getFiles().contains(filename))
                                                .map(Map.Entry::getKey)
                                                .collect(Collectors.toSet());
            
            CountDownLatch latch = new CountDownLatch(dStoresWithFile.size());
            awaitingRemove.put(filename, latch);

            // Send remove message to each Dstore
            dStoresWithFile.forEach(port -> dStores.get(port).getDstore().sendMessage(Protocol.REMOVE + " " + filename));

            try {
                if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    out.println(Protocol.REMOVE_COMPLETE);
                    index.remove(filename);
                    logger.info("Removal of file '" + filename + "' complete");
                } else { 
                    logger.err("Remove operation timed out for: '" + filename + "'"); 
                }
            } catch (Exception e) {
                logger.err("Error waiting for remove acks for: '" + filename + "'");
            } finally {
                awaitingRemove.remove(filename);
                rebalanceLock.readLock().unlock();
            }
        }
    }

    private static class UnknownHandler implements Runnable {
        private final Socket socket;

        //Logger
        private static final Logger logger = Logger.getLogger(UnknownHandler.class);

        public UnknownHandler(Socket socket) {
            this.socket = socket;
            logger.info("New Connection; port " + socket.getPort());
        }

        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String message;
                if ((message = in.readLine()) == null) return;
                switch (getOperation(message)) {
                    case Protocol.JOIN -> new Thread(new DstoreHandler(socket, Integer.parseInt(message.split(" ")[1]))).start();
                    default -> new Thread(new ClientHandler(socket, message)).start();
                }
            } catch (Exception e) { 
                logger.err("Error in Unknown Connection - Disconnected");
            } 
        }
    }

    private static class DstoreHandler extends ConnectionHandler {  
        private final Integer port;

        //Logger
        private static final Logger logger = Logger.getLogger(DstoreHandler.class);

        public DstoreHandler(Socket socket, Integer port) throws IOException {
            super(socket);
            this.port = port;
            
            dStores.put(port, new DstoreAndFiles(this));
            logger.info("New DStore Created; port " + socket.getPort() + ". (" + dStores.size() + "/" + replicationFactor + ") DStores connected");
            new Thread(new Rebalance()).start(); // Start Rebalance
        }

        public void sendMessage(String message) {
            out.println(message);
        }

        public void run() {
            try (socket; out; in) {
                String message;
                while((message = in.readLine()) != null) handleMessage(message);   
            } catch (Exception e) { } finally {
                dStores.remove(port);
                logger.info("Dstore Disconnected; (" + dStores.size() + "/" + replicationFactor + ") DStores remaining");
            }
        }

        public void handleMessage(String message) {
            logger.info("Message received from dStore; " + socket.getPort() + ": " + message);
            try {
                var arguments = message.split(" ");
                switch (getOperation(message)) {
                    case Protocol.STORE_ACK -> awaitingStore.get(arguments[1]).countDown();
                    case Protocol.REMOVE_ACK, Protocol.ERROR_FILE_DOES_NOT_EXIST -> {
                        var filename = arguments[1];
                        if (awaitingRemove.get(filename) == null) return; 
                        dStores.get(port).getFiles().remove(filename); // Remove file from dstore file list
                        awaitingRemove.get(filename).countDown();
                    }
                    case Protocol.LIST -> {
                        if (awaitingList == null || dStoreListFiles.containsKey(port)) return;
                        var files = new HashSet<>(Arrays.asList(Arrays.copyOfRange(arguments,1,arguments.length))); 
                        dStoreListFiles.put(port, files);
                        awaitingList.countDown();
                    }   
                    case Protocol.REBALANCE_COMPLETE -> awaitingRebalance.countDown();
                    default -> logger.err("Malformed Message from dstore");
                }
            } catch (Exception e) {
                logger.err("Error handling dstore message; port: " + socket.getPort());
            }
        }
    }

    private static String getOperation(String message) {
        return message.split(" ", 2)[0];
    }

    private static class DstoreAndFiles {
        private final DstoreHandler dStore;
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
    
    private static class Index {
        private final ConcurrentHashMap<String, FileInfo> index;

        public Index () {
            index = new ConcurrentHashMap<>();
        }

        public void put(String filename, Status status, Integer filesize) {
            index.put(filename, new FileInfo(status, filesize));
        }

        public void remove(String filename) {
            index.remove(filename);
        }

        public void setStatus(String filename, Status status) {
            index.get(filename).setStatus(status);
        }

        public Integer getSize(String filename) {
            return index.get(filename).getSize(); 
        }

        public Status getStatus(String filename) {
            return index.getOrDefault(filename, new FileInfo(Status.remove_complete, 0)).getStatus();
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
            return index.getOrDefault(filename, new FileInfo(Status.remove_complete, 0))
                .getStatus().equals(Status.store_complete);
        }

        public Integer sizeCompleted() {
            return index.entrySet().stream()
                .filter(e -> e.getValue().getStatus().equals(Status.store_complete))
                .collect(Collectors.toList()).size();
        }
    }

    private static class FileInfo {
        private Status status;
        private final Integer filesize;

        public FileInfo (Status status, Integer filesize) {
            this.status = status;
            this.filesize = filesize;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public Status getStatus() {
            return this.status;
        }

        public Integer getSize() {
            return this.filesize;
        }
    }

    enum Status {
        store_in_progress,
        store_complete,
        remove_in_progress,
        remove_complete
    }
}