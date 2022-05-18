import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public abstract class ConnectionHandler implements Runnable {
    protected final Socket socket;
    //Reader and Writers
    protected final PrintWriter out;
    protected final BufferedReader in;

    public ConnectionHandler(Socket socket) throws IOException {
        this.socket = socket;
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }
}