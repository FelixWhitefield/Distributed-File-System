import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.io.InputStreamReader;

public class Dstore {
    private static int port;
    private static int cport;
    private static int timeout;
    private static String fileFolder;

    private static Socket socket;

    //Reader and writer
    private static PrintWriter cout;
    private static BufferedReader cin;

    //Logger
    private static Logger logger = Logger.getLogger(Dstore.class.getName());

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

        try {
            Path folderPath = Paths.get(fileFolder);
            logger.info("Cleaning folder contents for: " + folderPath.toAbsolutePath().toString());
            if (Files.exists(folderPath)) 
                Files.walk(folderPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            Files.createDirectory(folderPath);
    
        } catch (IOException e1) {
            logger.err("Error cleaning file_folder contents");
            e1.printStackTrace();
            return;
        }

        try {
            connectToController();
        } catch (Exception e2) {
            logger.err("Error in connecting to server");
            e2.printStackTrace();
            return;
        }
    
        new ControllerHandler(socket).start();
        acceptConnections();
    }

    public static void connectToController() throws UnknownHostException, IOException {
        //Connect to server
        socket = new Socket("localhost", cport);
        //Load reader and writer
        cout = new PrintWriter(socket.getOutputStream(), true);
        cin = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        //Join dstore pool
        cout.println("JOIN " + port);
        logger.info("Connected to Controller");
    }

    public static void acceptConnections() {
        logger.info("Accepting connections");
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            for (;;) {
                Socket client = serverSocket.accept();
                client.setSoTimeout(timeout);
                new ClientHandler(client).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class ClientHandler extends Thread {
        private Socket socket;
        private PrintWriter out;
        private BufferedReader in;

        //Logger
        private static Logger logger = Logger.getLogger(ClientHandler.class.getName());

        private void closeConnection() {
            logger.info("Client disconnected");
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
        
        public ClientHandler(Socket socket) {
            this.socket = socket;
            logger.info("New Client Created");
        }

        public void run() {
             //Create reader and writer
             try {
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            } catch (Exception e) {
                logger.err("Could not create reader and/or writer");
                return;
            }
            
            
            try {
                while(!socket.isClosed()) {
                    String message = in.readLine();
                    if (message == null) {
                        break;
                    }
                    handleMessage(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
        }

        public void handleMessage(String message) {
            logger.info("Message received from client, " + socket.getPort() + ": " + message);

            try {
                var operation = getOperation(message);
                var arguments = message.split(" ");
                if (operation.equals("STORE")) {
                    var filename = arguments[1];
                    var filesize = Integer.parseInt(arguments[2]);

                    out.println("ACK"); // Notify client message received

                    store(filename, filesize);
                } else if (operation.equals("LOAD_DATA")) {
                    var filename = arguments[1];

                    load(filename);
                } else {
                    logger.err("Malformed message");
                }
            } catch (Exception e) {
                logger.err("Error in handling message");
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

                cout.println("STORE_ACK " + filename); // Notify Controller store complete
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
        Socket socket;

        //Logger
        private static Logger logger = Logger.getLogger(ControllerHandler.class.getName());

        public ControllerHandler(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            try {
                while (true) {
                    String message = cin.readLine();

                    if (message == null) {
                        return;
                    }
                    logger.info("Message Received: " + message);
                    handleMessage(message);
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            } finally { // Close socket gracefully
                logger.info("Server offline");

                try {
                    cout.close();
                } finally {
                    try {
                        cin.close();
                    } catch (IOException e) {} finally {
                        try {
                            socket.close();
                        } catch (IOException e) {} 
                    }
                    //END LIFE
                }
            }
        }

        public void handleMessage(String message) {
            try {
                String operation = getOperation(message);
                var arguments = message.split(" ");
    
                if (operation.equals("REMOVE")) {
                    var filename = arguments[1];

                    remove(filename);                    
                } else if (operation.equals("LIST")) {
                    list();
                }
            } catch (Exception e) {
                logger.err("Malformed Message");
            } 
        }

        public void list() {
            try (Stream<Path> stream = Files.list(Paths.get(fileFolder))) {
                var files = String.join(" ", stream.filter(file -> !Files.isDirectory(file))
                                .map(Path::getFileName)
                                .map(Path::toString)
                                .collect(Collectors.toList()));
                cout.println("LIST " + files);                
            } catch (Exception e) {}
        }

        public void remove(String filename) {
            logger.info("Removing file: " + filename);

            Path path = Paths.get(fileFolder, filename);
            try {
                if (Files.deleteIfExists(path)) {
                    cout.println(codes.remove_ack.value + " " + filename);
                } else {
                    cout.println(codes.error_file_does_not_exist.value + " " + filename);
                }
            } catch (IOException e) {
                logger.err("Error deleting file: " + filename);
            }
        }

    }

    private static String getOperation(String message) {
        return message.split(" ", 2)[0];
    }
    
    enum codes {
        error_file_does_not_exist("ERROR_FILE_DOES_NOT_EXIST"),
        remove_ack("REMOVE_ACK");

        private String value;
        codes(String value) { this.value = value; }
    }
}
