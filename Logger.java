import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {
    private Class<?> caller;


    public static Logger getLogger(Class<?> caller) {
        return new Logger(caller);
    }

    public Logger(Class<?> caller) {
        this.caller = caller;
    }

    public void info(Object message) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        String callerName = caller.getCanonicalName();

        System.out.println(sdf.format(date) + " | " + callerName + " | Thread " + Thread.currentThread().getId() + " | " + "INFO | " + String.valueOf(message));
    }

    public void err(Object message) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        String callerName = caller.getCanonicalName();

        System.err.print("\u001B[31m"); 
        System.err.println(sdf.format(date) + " | " + callerName + " | Thread " + Thread.currentThread().getId() + " | " + "ERROR | " + String.valueOf(message));
        System.err.print("\u001B[0m"); 
    }
    
}
