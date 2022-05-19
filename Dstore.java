import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.InputStreamReader;

public class Dstore {
    private static Integer port, cport, timeout;
    private static String fileFolder;

    //Logger
    private static final Logger logger = Logger.getLogger(Dstore.class);

    //Cnontroller Handler
    private static ControllerHandler controllerHandler;

    //Task Thread pool
    private static ExecutorService taskPool = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        try { //Parse inputs
            port = Integer.parseInt(args[0]);
            cport = Integer.parseInt(args[1]); 
            timeout = Integer.parseInt(args[2]); //ms
            fileFolder = args[3];
        } catch (Exception e) {
            logger.err("Malformed arguemnts; java Dstore port cport timeout file_folder");
            return;
        }

        try { // Clear Folder Contents
            Path folderPath = Paths.get(fileFolder);
            logger.info("Cleaning folder contents: " + folderPath.toAbsolutePath().toString());
            if (Files.exists(folderPath)) 
                Files.walk(folderPath)
                     .sorted(Comparator.reverseOrder())
                     .map(Path::toFile)
                     .forEach(File::delete);
            Files.createDirectory(folderPath);
        } catch (IOException e) {
            logger.err("Error cleaning folder: '" + fileFolder + "''");
            return;
        }

        try { // Connect To The Controller
            connectToController();
        } catch (Exception e) {
            logger.err("Error connecting to controller");
            return;
        }

        acceptConnections(); // Accept Connections From Clients and other Dstores
    }

    private static void connectToController() throws UnknownHostException, IOException {
        Socket socket = new Socket(InetAddress.getLocalHost(), cport);

        controllerHandler = new ControllerHandler(socket);
        new Thread(controllerHandler).start();
    }

    private static void acceptConnections() {
        logger.info("Accepting connections");
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            for (;;) {
                try {
                    Socket socket = serverSocket.accept();
                    taskPool.execute(new ClientHandler(socket));
                } catch (IOException e) { logger.err("Error Creating Client"); }
            }
        } catch (IOException e) {
            logger.err("Error Accepting Connections - Clients can no longer connect");
        }
    }

    private static class ClientHandler extends ConnectionHandler {
        //Logger
        private static final Logger logger = Logger.getLogger(ClientHandler.class);
        
        public ClientHandler(Socket socket) throws IOException {
            super(socket);

            socket.setSoTimeout(timeout);
            logger.info("New Client Created");
        }

        public void run() {
            try (socket; out; in) {
                String message;
                if ((message = in.readLine()) != null) handleMessage(message);
            } catch (Exception e) { } finally {
                logger.info("Client Disconnected");
            }
        }

        private void handleMessage(String message) {
            try {
                var arguments = message.split(" ");
                switch (getOperation(message)) {
                    case Protocol.STORE     -> store(arguments[1], Integer.parseInt(arguments[2]), false);
                    case Protocol.LOAD_DATA -> load(arguments[1]);
                    case Protocol.REBALANCE_STORE -> store(arguments[1], Integer.parseInt(arguments[2]), true);
                    default -> logger.err("Malformed client message: '" + message + "'");
                }
            } catch (Exception e) {
                logger.err("Error handling client/dstore message: '" + message + "'");
            }
        }

        private void store(String filename, Integer filesize, Boolean isRebalance) {
            logger.info("Store operation started for file: '" + filename + "'..");
            out.println(Protocol.ACK); // Notify client message received
            try {
                byte[] data = new byte[filesize];  //Get data from client
                socket.getInputStream().readNBytes(data, 0, filesize); 

                Path path = Paths.get(fileFolder, filename); //Store file in folder
                Files.write(path, data);

                if (!isRebalance) controllerHandler.sendMessage(Protocol.STORE_ACK + " " + filename); // Notify Controller store complete
                logger.info("Store of file '" + filename + "' complete");
            } catch (Exception e) {
                logger.err("Error storing file: '" + filename + "'");
            } 
        }

        private void load(String filename) {
            logger.info("Load operation started for file: '" + filename + "'..");
            try {
                Path path = Paths.get(fileFolder, filename);
                if (!Files.exists(path)) return; // If file doesn't exist
                byte[] data = Files.readAllBytes(path); // Read file

                socket.getOutputStream().write(data); // Send data
                logger.info("Load of file '" + filename + "' complete");
            } catch (Exception e) {
                logger.err("Error loading file: " + filename + "'");
            }
        }
    }

    private static class ControllerHandler extends ConnectionHandler {
        //Logger
        private static final Logger logger = Logger.getLogger(ControllerHandler.class);

        public ControllerHandler(Socket socket) throws IOException {
            super(socket);

            out.println("JOIN " + port);
            logger.info("Connected to Controller");
        }

        public void sendMessage(String message) {
            out.println(message);
        }

        public void run() {
            try (socket; in; out) {
                String message;
                while ((message = in.readLine()) != null) handleMessage(message);
            } catch (Exception e) { } finally { 
                logger.info("Server connection offline");
                System.exit(0);
            }
        }

        private void handleMessage(String message) {
            try {
                var arguments = message.split(" ");
                switch (getOperation(message)) {
                    case Protocol.LIST   -> list();
                    case Protocol.REMOVE -> remove(arguments[1]);   
                    case Protocol.REBALANCE -> {
                        var rebal = Arrays.asList(arguments);
                        HashMap<String, Set<Integer>> filesToSend = new HashMap<>();
                        ArrayList<String> filesToDelete = new ArrayList<>();
                        var iter = rebal.iterator();
                        iter.next();
                        // Get values from message
                        var noOfFiles = Integer.parseInt(iter.next());
                        IntStream.range(0, noOfFiles).forEach(i -> { 
                            var filename = iter.next();
                            var noOfPorts = Integer.parseInt(iter.next());
                            Set<Integer> ports = new HashSet<>();
                            IntStream.range(0, noOfPorts).forEach(j -> ports.add(Integer.parseInt(iter.next())));
                            filesToSend.put(filename, ports);
                        });
                        var noOfFilesDelete = Integer.parseInt(iter.next());
                        IntStream.range(0, noOfFilesDelete).forEach(i -> filesToDelete.add(iter.next()));
                        rebalance(filesToSend, filesToDelete);
                    }
                    default -> logger.err("Malformed controller message: '" + message + "'");
                }
            } catch (Exception e) {
                logger.err("Error handling controller message");
            } 
        }

        private void list() {
            logger.info("List operation started..");
            try (Stream<Path> stream = Files.list(Paths.get(fileFolder))) {
                var files = String.join(" ", stream.filter(file -> !Files.isDirectory(file))
                            .map(Path::getFileName).map(Path::toString)
                            .collect(Collectors.toList()));
                out.println((files.length() == 0) ? Protocol.LIST : Protocol.LIST + " " + files);    
                logger.info("List complete");         
            } catch (Exception e) {
                logger.err("Error in list operation");
            }
        }

        private void remove(String filename) {
            logger.info("Remove operation started for: '" + filename + "'..");
            Path path = Paths.get(fileFolder, filename);
            try {
                if (Files.deleteIfExists(path)) out.println(Protocol.REMOVE_ACK + " " + filename);
                else out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST + " " + filename);
                logger.info("Removal of file: '" + filename + "' complete");
            } catch (IOException e) {
                logger.err("Error removing file: " + filename);
            }
        }

        private void rebalance(HashMap<String, Set<Integer>> filesToSend, ArrayList<String> filesToDelete) {
            logger.info("Rebalance operation started..");
            Set<SendFile> allFilesSend = new HashSet<>();

            filesToSend.entrySet().stream().forEach(e -> {
                Integer filesize;
                try { filesize = (int) Files.size(Paths.get(fileFolder, e.getKey()));
                } catch (Exception e1) { return; }
                e.getValue().forEach(p ->  allFilesSend.add(new SendFile(e.getKey(), p, filesize)));
            });
            try {
                var results = taskPool.invokeAll(allFilesSend);
                var allSuccessful = results.stream().allMatch(e -> {
                    try { return e.get();
                    } catch (Exception ex) { return false; }
                });
                if (!allSuccessful) return;
            } catch (Exception e1) { return; }

            filesToDelete.forEach(f -> {
                try { 
                    logger.info("Deleting file '" + f + "'");
                    Files.deleteIfExists(Paths.get(fileFolder, f));
                } catch (Exception e) { return; }
            });
            out.println(Protocol.REBALANCE_COMPLETE);
            logger.info("Rebalance operation complete");
        }

        private static class SendFile implements Callable<Boolean> {
            private final String filename;
            private final Integer filesize;
            private final Integer port;
    
            public SendFile(String filename, Integer port, Integer filesize) {
                this.filename = filename;
                this.filesize = filesize;
                this.port = port;
            }

            @Override
            public Boolean call() {
                try (
                    Socket socket = new Socket(InetAddress.getLocalHost(), port);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                ) {
                    logger.info("Sending '" + filename + "' to: " + port);
                    socket.setSoTimeout(timeout);
    
                    out.println(Protocol.REBALANCE_STORE + " " + filename + " " + filesize);
                    if (in.readLine().equals(Protocol.ACK)) {
                        byte[] data = Files.readAllBytes(Paths.get(fileFolder, filename));
                        socket.getOutputStream().write(data);
                        return true;
                    } 
                } catch (Exception e) { 
                    logger.err("Error sending '" + filename + "' to: " + port);
                } 
                return false;
            }
        }
    }

    private static String getOperation(String message) {
        return message.split(" ", 2)[0];
    }    
}