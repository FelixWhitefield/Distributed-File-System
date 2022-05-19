import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {
    private Class<?> caller;
    private String callerName;
    private SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

    public static Logger getLogger(Class<?> caller) {
        return new Logger(caller);
    }

    public Logger(Class<?> caller) {
        this.caller = caller;
        this.callerName = caller.getCanonicalName();
    }

    public void info(Object message) {
        Date date = new Date();

        System.out.println(sdf.format(date) + " | " + callerName + " | Thread " 
            + Thread.currentThread().getId() + " | " + "INFO | " + String.valueOf(message));
    }

    public void err(Object message) {
        Date date = new Date();

        System.err.println(sdf.format(date) + " | " + callerName + " | Thread " 
            + Thread.currentThread().getId() + " | " + "ERROR | " + String.valueOf(message));
    }
}