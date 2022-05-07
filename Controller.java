import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectStreamConstants;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collector;
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

    //Blocking queues
    private static ConcurrentHashMap<String, BlockingQueue<Integer>> awaitingStoreAcks = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, BlockingQueue<Integer>> awaitingRemoveAcks = new ConcurrentHashMap<>();
    private static BlockingQueue<Entry<Integer,String>> awaitingList;

    //Locks
    private static ReentrantLock statusCheckLock = new ReentrantLock();
    private static ReentrantReadWriteLock rebalanceLock = new ReentrantReadWriteLock();

    //Logger
    private static Logger logger = Logger.getLogger(Controller.class.getName());


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

        logger.info("Accepting Connections");
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.schedule(new Rebalance(), 10, TimeUnit.SECONDS);
        acceptConnections();
    }

    public static void acceptConnections() {
        try (ServerSocket serverSocket = new ServerSocket(cport);) {
            for (;;) {
                Socket client = serverSocket.accept();
                new UnknownHandler(client).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Rebalance implements Runnable {
        private BlockingQueue<Map.Entry<Integer, String>> listQueue = new LinkedBlockingQueue<>();

        @Override
        public void run() {
            logger.info("Attemping rebalance");
            rebalanceLock.writeLock().lock();

            //ATTEMPT TO GET LIST FROM EACH DSTORE
            try {
                awaitingList = listQueue;
                for (DstoreAndFiles dstore : dStores.values()) {
                    try {
                        dstore.getDstore().sendMessage("LIST");
                    } catch (Exception e) {}
                }

                HashMap<Integer, ArrayList<String>> dStoreFiles;
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Future<HashMap<Integer, ArrayList<String>>> future = executor.submit(new listWaiter(new HashSet<>(dStores.keySet())));
                try {
                    dStoreFiles = future.get(timeout, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    future.cancel(true);
                    return;
                } finally {
                    awaitingList = null;
                    executor.shutdown();
                }

                //COMPARE FILES FROM DSTORE TO FILES LIST THAT CONTROLLER HAS
                HashMap<Integer, ArrayList<String>> extraFiles = new HashMap<>(); // Extra files the dstores have (not in controller list)
                dStoreFiles.entrySet().stream().forEach(e -> extraFiles.put(e.getKey(), new ArrayList<>(e.getValue().stream().filter(f -> dStores.get(e.getKey()).getFiles().contains(f)).collect(Collectors.toList()))));
                HashMap<Integer, ArrayList<String>> missingFiles = new HashMap<>(); // Files which the dstores dont have
                dStores.entrySet().stream().forEach(e -> missingFiles.put(e.getKey(), new ArrayList<>(e.getValue().getFiles().stream().filter(f -> !dStoreFiles.get(e.getKey()).contains(f)).collect(Collectors.toList()))));         
                
                for (Entry<Integer, ArrayList<String>> entry : missingFiles.entrySet()) { // Remove files which dstores do not have
                    dStores.get(entry.getKey()).getFiles().removeAll(entry.getValue());
                    logger.info(entry.getKey() + entry.getValue().toString());
                }
                ArrayList<String> filesNotStored = new ArrayList<>();
                for (String file : index.keySet()) { // Check that each file in index is stored at least once (if not remove from index)
                    var anyContain = dStores.entrySet().stream().anyMatch(e -> e.getValue().getFiles().contains(file));
                    if (!anyContain) filesNotStored.add(file);
                }
                index.keySet().removeAll(filesNotStored);
                HashMap<Integer, ArrayList<String>> filesToRemove = new HashMap<>();
                for (var dstorePort : dStores.keySet()) { // Populate with empty array
                    filesToRemove.put(dstorePort, new ArrayList<>());
                }
                
                // Attempt to fix remove in progress files
                var filesRemovingInProgress = index.entrySet().stream()
                    .filter(e -> Objects.equals(e.getValue().getStatus(), status.remove_in_progress))
                    .map(Map.Entry::getKey).collect(Collectors.toList());

                for (String filename : filesRemovingInProgress) {
                    new attemptRemoveInProgress(filename).start();
                }
                //Rebalance now
                Boolean balanced = dStores.entrySet().stream()
                    .map(e -> e.getValue().getFiles().stream().filter(f -> index.isStoreComplete(f)).collect(Collectors.toList()).size())
                    .allMatch(e -> (e >= Math.floor((replicationFactor * index.sizeCompleted())/(float)dStores.size())) 
                        || (e >= Math.ceil((replicationFactor * index.sizeCompleted())/(float)dStores.size())));

                if (balanced) return;

                HashMap<Integer, HashMap<Integer, String>> filesToSend = new HashMap<>();
                HashMap<Integer, ArrayList<String>> filesToDelete = new HashMap<>();
                var dStoreCopy = dStores.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> List.copyOf(e.getValue().getFiles())));
                // Ensure each file is replicated R times
                for (String file : index.keySet()) {
                    var noOfCopies = dStoreCopy.values().stream().filter(e -> e.contains(file)).collect(Collectors.toList()).size();
                    logger.info(noOfCopies);
                }


                // while (!dStoreCopy.entrySet().stream()
                //             .map(e -> e.getValue().stream().filter(f -> index.isStoreComplete(f)).collect(Collectors.toList()).size())
                //             .allMatch(e -> (e >= Math.floor((replicationFactor * index.sizeCompleted())/(float)dStores.size())) 
                //                 || (e >= Math.ceil((replicationFactor * index.sizeCompleted())/(float)dStores.size())))) {
                //     ArrayList<Integer> dStoresSorted =  new ArrayList<>(dStoreCopy.keySet().stream()
                //                 .sorted(Comparator.comparingInt(e -> dStoreCopy.get(e).size())).collect(Collectors.toList()));
                //     var fileToMove = (dStoreCopy.get(dStoresSorted.get(dStoresSorted.size() - 1)))
                //                     .stream().filter(e -> (dStoreCopy.get(dStoresSorted.get(0)).contains(e)))
                //                     .limit(0).collect(Collectors.toList()).get(0);
                //     //filesToDelete.
                // }
            } catch (Exception e) {
                logger.err("Error rebalancing");
            } finally {
                logger.info("Rebalance complete");
                rebalanceLock.writeLock().unlock();
            }
        }

        public class attemptRemoveInProgress extends Thread {
            String filename;

            public attemptRemoveInProgress(String filename) {
                this.filename = filename;
            }

            @Override
            public void run() {
                ArrayList<Integer> dStoresWithFile = new ArrayList<>(dStores.entrySet().stream()
                .filter(e -> e.getValue().getFiles().contains(filename))
                .map(Map.Entry::getKey).collect(Collectors.toList()));
                
                if (dStoresWithFile.isEmpty()) index.remove(filename);
                else {
                    var ackQueue = new LinkedBlockingQueue<Integer>();
                    awaitingRemoveAcks.put(filename, ackQueue);
                    for (var dStorePort : dStoresWithFile) {
                        dStores.get(dStorePort).getDstore().sendMessage("REMOVE " + filename);
                    }
                    if (waitForRemoveAcks(dStoresWithFile, filename, ackQueue)) {
                        index.remove(filename);
                    } 
                    awaitingRemoveAcks.remove(filename);
                } 
            }
        }
    
        public class listWaiter implements Callable<HashMap<Integer, ArrayList<String>>> {
            Set<Integer> dStoresToList;
            public listWaiter(Set<Integer> dStoresToList) {
                this.dStoresToList = dStoresToList;
            }

            @Override
            public HashMap<Integer, ArrayList<String>> call() throws InterruptedException {
                HashMap<Integer, ArrayList<String>> dStoreFiles = new HashMap<>();
                while (true) {
                    Entry<Integer, String> listMessage = listQueue.take();
                    var dstorePort = listMessage.getKey();
                    var files = new ArrayList<>(Arrays.asList(listMessage.getValue().split(" "))); 
                    dStoreFiles.put(dstorePort, files);
                    dStoresToList.remove(dstorePort);
                    if (dStoresToList.isEmpty()) return dStoreFiles;
                }
            }           
        }
    }
    
    

    private static class ClientHandler extends Thread {
        private Socket socket;
        private ArrayList<Integer> attemptedLoad = new ArrayList<>();

        //Reader and Writers
        private PrintWriter out;
        private BufferedReader in;

        private BlockingQueue<Integer> ackQueue = new LinkedBlockingQueue<>();

        //Logger
        private static Logger logger = Logger.getLogger(ClientHandler.class.getName());
    
        private void closeConnection() { // Close socket gracefully
            logger.info("Client Disconnected");
            try { 
                out.close();
            } catch (Exception e) {} finally { 
                try {
                    in.close();
                } catch (Exception e) {} finally {
                    try {
                        socket.close();
                    } catch (Exception e) {}
                }
            }
        }

        public ClientHandler(Socket socket, String message) {
            this.socket = socket;

            //Create reader and writer
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            } catch (Exception e) {
                logger.err("Could not create reader and/or writer");
                return;
            }
           
            logger.info("New Client Created");
            handleMessage(message);
            
        }
    
        public void run() {
            try {
                if (out == null || in == null) { // If reader or writer could not be created
                    return;
                }

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
            } finally {
                closeConnection();
            }
        }

        public void handleMessage(String message) { 
            logger.info("Message received from client, "+ socket.getPort() + ": " + message);
            try {
                var operation = getOperation(message);
                var arguments = message.split(" ");

                if (operation.equals("LIST")) {
                    list();
                } else if (operation.equals("STORE")) {
                    var filename = arguments[1];
                    var filesize = Integer.parseInt(arguments[2]);

                    store(filename, filesize);
                } else if (operation.equals("LOAD")) {
                    var filename = arguments[1];
                    if (!attemptedLoad.isEmpty()) attemptedLoad.clear();
                    load(filename);
                    return;
                } else if (operation.equals("RELOAD")) {
                    var filename = arguments[1];

                    load(filename);
                    return;
                }
                else if (operation.equals("REMOVE")) {
                    var filename = arguments[1];

                    remove(filename);
                } else {
                    logger.err("Malformed Message from client");
                }
                attemptedLoad.clear();
            } catch (Exception e) {
                logger.err("Error in handling message");
                e.printStackTrace();
            }
            
        }

        private void list() {
            if (dStores.size() < replicationFactor) {
                out.println(codes.error_not_enough_dstores.value);
                logger.err("NOT ENOUGH DSTORES");
                return;
            }

            String files = String.join(" ", index.entrySet() // Only list the stored files
                    .stream()
                    .filter(e -> Objects.equals(e.getValue().getStatus(), status.store_complete))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet()));
            out.println("LIST " + files);
        }

        private void store(String filename, Integer filesize) {       
            if (dStores.size() < replicationFactor) {
                out.println(codes.error_not_enough_dstores.value);
                logger.err("NOT ENOUGH DSTORES");
                return;
            }

            statusCheckLock.lock();
            if (index.containsFile(filename)) {
                out.println(codes.error_file_already_exists.value);
                logger.err("FILE ALREADY EXISTS");
                return;
            } 
            rebalanceLock.readLock().lock();
            index.put(filename, status.store_in_progress, filesize); // Add status to index, start processing
            statusCheckLock.unlock();


            ArrayList<Integer> dStoresToUse = 
                    new ArrayList<>(dStores.keySet().stream()
                    .sorted(Comparator.comparingInt(e -> dStores.get(e).getFiles().size()))
                    .limit(replicationFactor).collect(Collectors.toList()));

            String toStore = 
                String.join(" ",  dStoresToUse.stream().map(String::valueOf).collect(Collectors.toList()));
            
            //Put ack queue in array
            awaitingStoreAcks.put(filename, ackQueue);

            logger.info("Storing to: " + toStore);
            out.println("STORE_TO " + toStore);

            //Check for acks from dstores
            if (waitForStoreAcks(new ArrayList<>(dStoresToUse), filename)) {
                out.println(codes.store_complete.value);
                index.setStatus(filename, status.store_complete);
                for (Integer dStorePort : dStoresToUse) {
                    dStores.get(dStorePort).getFiles().add(filename);
                }
            } else {
                logger.err("Error Storing File");
                index.remove(filename);
            }
            awaitingStoreAcks.remove(filename);
            rebalanceLock.readLock().unlock();
        }

        public Boolean waitForStoreAcks(ArrayList<Integer> dStoresToUse, String filename) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<Boolean> future = executor.submit(new storeAckWaiter(dStoresToUse, filename));

            try {
                return future.get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                future.cancel(true);
                return false;
            } finally {
                executor.shutdown();
            }
        }

        public class storeAckWaiter implements Callable<Boolean> {
            ArrayList<Integer> dStoresToUse;
            String filename;

            public storeAckWaiter (ArrayList<Integer> dStoresToUse, String filename){
                this.dStoresToUse = dStoresToUse;
                this.filename = filename;
            }
            @Override
            public Boolean call() throws InterruptedException {
                while (true) {
                    Integer dStoreAck = ackQueue.take();
                    dStoresToUse.remove(dStoreAck);
                    if (dStoresToUse.isEmpty()) return true;
                }           
            }
        }

        private void load(String filename) { // REMOVE MAY START WHEN LOAD IS IN PROGRESS?
            if (dStores.size() < replicationFactor) {
                out.println(codes.error_not_enough_dstores.value);
                logger.err("NOT ENOUGH DSTORES");
                return;
            }


            if (!index.getStatus(filename).equals(status.store_complete)) {
                out.println(codes.error_file_does_not_exist.value);
                logger.err("ERROR FILE DOES NOT EXIST");
                return;
            } 
            
            List<Integer> dStoresWithFile = dStores.entrySet().stream()
                                                    .filter(e -> e.getValue().getFiles().contains(filename))
                                                    .map(Map.Entry::getKey)
                                                    .collect(Collectors.toList());

            dStoresWithFile.removeAll(attemptedLoad);
            if (dStoresWithFile.isEmpty()) {
                out.println(codes.error_load.value);
                logger.err("ERROR LOADING FROM DSTORES");
                return;
            } 
            Integer dStorePort = dStoresWithFile.get(0);
            out.println("LOAD_FROM " + dStorePort + " " + index.getSize(filename)); 
            logger.info("Attempting to load from dStore with port: " + dStorePort);
            attemptedLoad.add(dStorePort);
        }

        private void remove(String filename) {
            if (dStores.size() < replicationFactor) {
                out.println(codes.error_not_enough_dstores.value);
                logger.err("NOT ENOUGH DSTORES");
                return;
            }

            statusCheckLock.lock();
            if (!index.getStatus(filename).equals(status.store_complete)) {
                out.println(codes.error_file_does_not_exist.value);
                logger.err("ERROR FILE DOES NOT EXIST");
                return;
            }
            rebalanceLock.readLock().lock();
            index.setStatus(filename, status.remove_in_progress);  // Add status to index, start processing
            statusCheckLock.unlock();

            
            ArrayList<Integer> dStoresWithFile = new ArrayList<>(dStores.entrySet().stream()
                                                .filter(e -> e.getValue().getFiles().contains(filename))
                                                .map(Map.Entry::getKey)
                                                .collect(Collectors.toList()));
            
            awaitingRemoveAcks.put(filename, ackQueue);

            for (Integer dStorePort : dStoresWithFile) {
                dStores.get(dStorePort).getDstore().sendMessage("REMOVE " + filename);
            }      
             
            if (waitForRemoveAcks(dStoresWithFile, filename, ackQueue)) {
                out.println(codes.remove_complete.value);
                index.remove(filename);
            } 
            awaitingRemoveAcks.remove(filename);
            rebalanceLock.readLock().unlock();
        }
    }

    public static Boolean waitForRemoveAcks(ArrayList<Integer> dStoresWithFile, String filename, BlockingQueue<Integer> queue) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(new removeAckWaiter(dStoresWithFile, filename, queue));

        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            future.cancel(true);
            return false;
        } finally {
            executor.shutdown();
        }
    }

    public static class removeAckWaiter implements Callable<Boolean> {
        ArrayList<Integer> dStoresWithFile;
        String filename;
        BlockingQueue<Integer> ackQueue;

        public removeAckWaiter (ArrayList<Integer> dStoresWithFile, String filename, BlockingQueue<Integer> queue){
            this.dStoresWithFile = dStoresWithFile;
            this.filename = filename;
            this.ackQueue = queue;
        }
        @Override
        public Boolean call() throws InterruptedException {
            while (true) {
                Integer dStoreAck = ackQueue.take();
                dStoresWithFile.remove(dStoreAck);
                dStores.get(dStoreAck).getFiles().remove(filename);
                if (dStoresWithFile.isEmpty()) return true;
            }           
        }
    }


    private static class UnknownHandler extends Thread {
        private Socket socket;

        //Logger
        private static Logger logger = Logger.getLogger(UnknownHandler.class.getName());

        public UnknownHandler(Socket socket) {
            this.socket = socket;
            logger.info("New Connection; port " + socket.getPort());
        }

        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String message = in.readLine();
                if (message == null) {
                    logger.info("Unknown Connection Lost");
                    return;
                }

                String operation = getOperation(message);

                if (operation.equals("JOIN")) {
                    var arguments = message.split(" ");
                    Integer port = Integer.parseInt(arguments[1]);
                    new DstoreHandler(socket, port).start();
                } else {
                    ClientHandler t = new ClientHandler(socket, message);
                    t.start();
                    synchronized(t) {
                        t.wait();
                        Thread.sleep(1000);
                        t.notify();
                    }
                }

            } catch (Exception e) {
                logger.info("Unknown Connection Lost or Malformed Message");
                e.printStackTrace();
            }
        }
    }

    private static class DstoreHandler extends Thread {
        private Socket socket;
        private Integer port;

        //Reader and Writers
        private PrintWriter out;
        private BufferedReader in;

        //Logger
        private static Logger logger = Logger.getLogger(DstoreHandler.class.getName());

        private void closeConnection() {
            logger.info("DStore Disconnected; port " + socket.getPort() + ". (" + dStores.size() + "/" + replicationFactor + ") DStores remaining");
            try { // Close socket gracefully
                out.close();
            } catch (Exception e) {} finally { 
                try {
                    in.close();
                } catch (Exception e) {} finally {
                    try {
                        socket.close();
                    } catch (Exception e) {}
                }
            }
        }

        public DstoreHandler(Socket socket, Integer port) {
            this.socket = socket;
            this.port = port;

            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            } catch (Exception e) {
                logger.err("Could not create reader and/or writer");
                return;
            }
            
            dStores.put(port, new DstoreAndFiles(this));
            logger.info("New DStore Created; port " + socket.getPort() + ". (" + dStores.size() + "/" + replicationFactor + ") DStores connected");

        }

        public void sendMessage(String message) {
            out.println(message);;
        }

        public void run() {
            try {
                if (out == null || in == null) { // If reader or writer could not be created
                    return;
                }
            
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
                logger.err("Error in Dstore: " + e);
            } finally {
                dStores.remove(port);
                closeConnection();
            }

        }

        public void handleMessage(String message) {
            logger.info("Message received from dStore, " + socket.getPort() + ": " + message);
            try {
                var operation = getOperation(message);
                var arguments = message.split(" ");

                if (operation.equals("STORE_ACK")) {
                    var filename = arguments[1];

                    awaitingStoreAcks.get(filename).put(port);
                } else if (operation.equals("REMOVE_ACK") || operation.equals("ERROR_FILE_DOES_NOT_EXIST")) {
                    var filename = arguments[1];

                    awaitingRemoveAcks.get(filename).put(port);
                } else if (operation.equals("LIST")) {
                    awaitingList.put(Map.entry(port, message.split(" ", 2)[1]));
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

    enum codes {
        store_complete("STORE_COMPLETE"),
        remove_complete("REMOVE_COMPLETE"),
        error_not_enough_dstores("ERROR_NOT_ENOUGH_DSTORES"),
        error_file_already_exists("ERROR_FILE_ALREADY_EXISTS"),
        error_file_does_not_exist("ERROR_FILE_DOES_NOT_EXIST"),
        error_load("ERROR_LOAD");

        private String value;
        codes(String value) { this.value = value; }
    }
}
 