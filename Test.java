import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Test {
    static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    static ReentrantLock clock = new ReentrantLock();



    public static void main(String[] args) {
        
        
        

        



        new Worker().start();
        new Worker().start();

        
        try {
            Thread.sleep(300);
        } catch (Exception e) {

        }
        clock.lock();
        System.out.println("STARTED");

        try {
            Thread.sleep(3000);
            clock.unlock();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println("DONE");
    }


    public static class Worker extends Thread {

        @Override
        public void run() {
            System.out.println("WAITING " + getId());
            clock.lock();

            try {
                System.out.println("STARTED " + this.getId());
                Thread.sleep(5000);
                System.out.println("ENDED " + this.getId());
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            clock.unlock();
        }
    }
}
