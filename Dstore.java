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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.InputStreamReader;

public class Dstore {
    private static int port;
    private static int cport;
    private static int timeout;
    private static String fileFolder;

    //Logger
    private static Logger logger = Logger.getLogger(Dstore.class);

    //Cnontroller Handler
    private static ControllerHandler controllerHandler;

    public static void main(String[] args) {
        try { //Parse inputs
            port = Integer.parseInt(args[0]);
            cport = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            fileFolder = args[3];
        } catch (Exception e) {
            logger.err("Malformed arguemnts; java Dstore port cport timeout file_folder");
            return;
        }

        try { // Clear Folder Contents
            Path folderPath = Paths.get(fileFolder);
            logger.info("Cleaning folder contents for: " + folderPath.toAbsolutePath().toString());
            if (Files.exists(folderPath)) 
                Files.walk(folderPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            Files.createDirectory(folderPath);
        } catch (IOException e) {
            logger.err("Error cleaning folder '" + fileFolder + "'' contents");
            e.printStackTrace();
            return;
        }

        try { // Connect To The Controller
            connectToController();
        } catch (Exception e) {
            logger.err("Error connecting to server");
            e.printStackTrace();
            return;
        }

        acceptConnections(); // Accept Connections From Clients and Dstores
    }

    public static void connectToController() throws UnknownHostException, IOException {
        Socket socket = new Socket(InetAddress.getLocalHost(), cport);
        logger.info("Connected to Controller");

        controllerHandler = new ControllerHandler(socket);
        controllerHandler.start();
    }

    public static void acceptConnections() {
        logger.info("Accepting connections");
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            for (;;) {
                try {
                    Socket client = serverSocket.accept();
                    client.setSoTimeout(timeout);
                    new ClientHandler(client).start();
                } catch (IOException e) { logger.err("Error Creating Client"); }
            }
        } catch (IOException e) {
            logger.err("Error Accepting Connections - Clients can no longer connect");
            e.printStackTrace();
        }
    }

    private static class ClientHandler extends Thread {
        private final Socket socket;
        private final PrintWriter out;
        private final BufferedReader in;

        //Logger
        private static Logger logger = Logger.getLogger(ClientHandler.class);
        
        public ClientHandler(Socket socket) throws IOException {
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            logger.info("New Client Created");
        }

        public void run() {
            try (socket; out; in) {
                while(!socket.isClosed()) {
                    String message = in.readLine();
                    if (message == null) {
                        return;
                    }
                    handleMessage(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } 
        }

        public void handleMessage(String message) {
            logger.info("Message received from client, " + socket.getPort() + ": " + message);

            try {
                var arguments = message.split(" ");

                switch (getOperation(message)) {
                    case Protocol.STORE -> {
                        var filename = arguments[1];
                        var filesize = Integer.parseInt(arguments[2]);
                        out.println(Protocol.ACK); // Notify client message received
                        store(filename, filesize);
                    }
                    case Protocol.LOAD_DATA -> {
                        var filename = arguments[1];
                        load(filename);
                    }
                    case Protocol.REBALANCE_STORE -> {
                        var filename = arguments[1];
                        var filesize = Integer.parseInt(arguments[2]);
                        out.println(Protocol.ACK); // Notify dstore message received
                        rebalanceStore(filename, filesize);
                    }
                    default -> logger.err("Malformed message");
                }
            } catch (Exception e) {
                logger.err("Error in handling message");
            }
        }

        public void rebalanceStore(String filename, Integer filesize) {
            try {
                //Get data from dstore
                byte[] data = new byte[filesize];
                socket.getInputStream().readNBytes(data, 0, filesize);

                //Store data in folder
                Path path = Paths.get(fileFolder, filename);
                Files.write(path, data);

                logger.info("Store of file '" + filename + "' complete");
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
                logger.err("Error in storing file");
            } 
        }

        public void store(String filename, Integer filesize) {
            try {
                //Get data from client
                byte[] data = new byte[filesize];
                socket.getInputStream().readNBytes(data, 0, filesize);

                //Store data in folder
                Path path = Paths.get(fileFolder, filename);
                Files.write(path, data);

                controllerHandler.sendMessage(Protocol.STORE_ACK + " " + filename); // Notify Controller store complete
                logger.info("Store of file '" + filename + "' complete");
            } catch (Exception e) {
                e.printStackTrace();
                logger.err("Error in storing file");
            } 
        }

        public void load(String filename) {
            try {
                Path path = Paths.get(fileFolder, filename);
                if (!Files.exists(path)) { // Check if it exists
                    socket.close();
                    return;
                }
                byte[] data = Files.readAllBytes(path);
                socket.getOutputStream().write(data);
            } catch (Exception e) {
                e.printStackTrace();
                logger.err("Error in loading file");
            }
        }
    }


    private static class ControllerHandler extends Thread {
        private final Socket socket;
        private final PrintWriter cout;
        private final BufferedReader cin;
        //Logger
        private static Logger logger = Logger.getLogger(ControllerHandler.class);

        public ControllerHandler(Socket socket) throws IOException {
            this.socket = socket;
            this.cout = new PrintWriter(socket.getOutputStream(), true);
            this.cin = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            cout.println("JOIN " + port);
        }

        public void sendMessage(String message) {
            cout.println(message);
        }

        public void run() {
            try (socket; cin; cout) {
                while (true) {
                    String message = cin.readLine();
                    if (message == null) {
                        return;
                    }

                    logger.info("Message Received From Controller: " + message);
                    handleMessage(message);
                }
            } catch (Exception e) {
                logger.err("Error Reading Message");
            } finally { 
                logger.info("Server connection offline");
                System.exit(0);
            }
        }

        public void handleMessage(String message) {
            try {
                var arguments = message.split(" ");

                switch (getOperation(message)) {
                    case Protocol.LIST -> list();
                    case Protocol.REMOVE -> {
                        var filename = arguments[1];
                        remove(filename);   
                    }
                    case Protocol.REBALANCE -> {
                        var rebal = Arrays.asList(arguments);
                        HashMap<String, Set<Integer>> filesToSend = new HashMap<>();
                        ArrayList<String> filesToDelete = new ArrayList<>();
                        var iter = rebal.iterator();
                        iter.next();
                        // Get values from message
                        var noOfFiles = Integer.parseInt(iter.next());
                        for (int i = 0; i < noOfFiles; i++) {
                            var filename = iter.next();
                            var noOfPorts = Integer.parseInt(iter.next());
                            Set<Integer> ports = new HashSet<>();
                            for (int j = 0; j < noOfPorts; j++) {
                                ports.add(Integer.parseInt(iter.next()));
                            }
                            filesToSend.put(filename, ports);
                        }
                        var noOfFilesDelete = Integer.parseInt(iter.next());
                        for (int i = 0; i < noOfFilesDelete; i ++) {
                            filesToDelete.add(iter.next());
                        }
                        rebalance(filesToSend, filesToDelete);
                    }
                    default -> logger.err("Malformed Message");
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.err("Error handling message");
            } 
        }

        public void list() {
            try (Stream<Path> stream = Files.list(Paths.get(fileFolder))) {
                var files = String.join(" ", stream.filter(file -> !Files.isDirectory(file))
                            .map(Path::getFileName)
                            .map(Path::toString)
                            .collect(Collectors.toList()));
                cout.println(Protocol.LIST + " " + files);                
            } catch (Exception e) {}
        }

        public void remove(String filename) {
            logger.info("Removing file: " + filename);

            Path path = Paths.get(fileFolder, filename);
            try {
                if (Files.deleteIfExists(path)) cout.println(Protocol.REMOVE_ACK + " " + filename);
                else cout.println(Protocol.ERROR_FILE_DOES_NOT_EXIST + " " + filename);
            } catch (IOException e) {
                logger.err("Error deleting file: " + filename);
            }
        }

        public void rebalance(HashMap<String, Set<Integer>> filesToSend, ArrayList<String> filesToDelete) {
            ForkJoinPool pool = ForkJoinPool.commonPool();
            List<SendFile> allFilesSend = new ArrayList<>();

            filesToSend.entrySet().stream().forEach(e -> {
                Integer filesize;
                try {
                    filesize = (int) Files.size(Paths.get(fileFolder, e.getKey()));
                } catch (Exception e1) { return; }
                e.getValue().forEach(p -> {
                    allFilesSend.add(new SendFile(e.getKey(), p, filesize));
                });
            });
            try {
                var results = pool.invokeAll(allFilesSend);
                var allSuccessful = results.stream().allMatch(e -> {
                    try {
                        return e.get();
                    } catch (Exception ex) { return false; }
                });
                if (!allSuccessful) return;
            } catch (Exception e1) { return; }

            filesToDelete.forEach(f -> {
                try {
                    Files.deleteIfExists(Paths.get(fileFolder, f));
                } catch (Exception e) { return; }
            });
            cout.println(Protocol.REBALANCE_COMPLETE);
        }

        public static class SendFile implements Callable<Boolean> {
            String filename;
            Integer filesize;
            Integer port;
    
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
                    socket.setSoTimeout(timeout);
    
                    out.println(Protocol.REBALANCE_STORE + " " + filename + " " + filesize);
                    if (in.readLine().equals(Protocol.ACK)) {
                        byte[] data = Files.readAllBytes(Paths.get(fileFolder, filename));
                        socket.getOutputStream().write(data);
                        logger.info("'" + filename + "' sent to: " + port);
                        return true;
                    } 
                } catch (IOException e) { 
                    e.printStackTrace();
                } 
                return false;
            }
        }
    }

    private static String getOperation(String message) {
        return message.split(" ", 2)[0];
    }
    
}
