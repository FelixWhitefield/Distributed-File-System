import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.text.AttributeSet.ColorAttribute;
import javax.swing.text.StyleConstants.ColorConstants;

public class Logger {
    private String caller;

    public static Logger getLogger(String caller) {
        return new Logger(caller);
    }

    public Logger(String caller) {
        this.caller = caller;
    }

    public void info(Object message) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        System.out.println(sdf.format(date) + " | " + caller + " | Thread " + Thread.currentThread().getId() + " | " + "INFO | " + String.valueOf(message));
    }

    public void err(Object message) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
         
        System.err.println(sdf.format(date) + " | " + caller + " | Thread " + Thread.currentThread().getId() + " | " + "ERROR | " + String.valueOf(message));
    }
    
}
