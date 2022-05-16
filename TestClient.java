import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TestClient {
    
    public static void main(String[] args) {
        
        try {
            Client client = new Client(12345, 10000, Logger.LoggingType.ON_TERMINAL_ONLY);
            client.connect();
            System.out.println("Connected");

            

            BufferedReader stdIn =
            new BufferedReader(
                new InputStreamReader(System.in));

            File file = new File("testfile");

            client.store(file);

            File file1 = new File ("testfile1");

            File file2 = new File ("testfile2");
            File file3 = new File ("testfile3");
            File file4 = new File ("testfile4");


            String userInput;
            while ((userInput = stdIn.readLine()) != null) {
                client.send(userInput);
                client.store(file);
                client.store(file1);
                client.store(file2);
                client.store(file3);
                client.store(file4);

                for (var s : client.list()) {
                    System.out.println(s);
                }

                Thread.sleep(20000);
                client.remove("testfile");

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
