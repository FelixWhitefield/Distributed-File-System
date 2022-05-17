import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {
    private Class<?> caller;
    private SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

    public static Logger getLogger(Class<?> caller) {
        return new Logger(caller);
    }

    public Logger(Class<?> caller) {
        this.caller = caller;
    }

    public void info(Object message) {
        Date date = new Date();

        String callerName = caller.getCanonicalName();

        System.out.println(sdf.format(date) + " | " + callerName + " | Thread " + Thread.currentThread().getId() + " | " + "INFO | " + String.valueOf(message));
    }

    public void err(Object message) {
        Date date = new Date();

        String callerName = caller.getCanonicalName();

        System.err.println("\u001B[31m" + sdf.format(date) + " | " + callerName + " | Thread " 
            + Thread.currentThread().getId() + " | " + "ERROR | " + String.valueOf(message) + "\u001B[0m");
    }
    
}
