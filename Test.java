import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Test {
    static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    static ReentrantLock clock = new ReentrantLock();

    public static class longMethod implements Runnable {

        @Override
        public void run() {
            System.out.println("START");
            try {
                System.out.println("HII");
                for (int i = 0; i < 10; i ++) {
                    System.out.println("INSIDE");
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();

            }
            System.out.println("HELLO");
        }
        
    }
    



    public static void main(String[] args) throws InterruptedException {

        BlockingQueue<Integer> n = new LinkedBlockingQueue<Integer>(2);

        n.add(2);

        System.out.println(n.size());

        System.out.println("BEG");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> rebalanceTask;

        rebalanceTask = scheduler.schedule(new longMethod(), 0, TimeUnit.MICROSECONDS);

        Thread.sleep(20);

        System.out.println(rebalanceTask.isDone());

        System.out.println(rebalanceTask.cancel(false));



        // System.out.println(Runtime.getRuntime().availableProcessors());


        // CountDownLatch latch = new CountDownLatch(2);
        // latch.countDown();
        // //latch.countDown();
        // System.out.println(latch.getCount());

        // try {
        //     System.out.println(Files.size(Paths.get("testfile")));
        // } catch (IOException e1) {
        //     // TODO Auto-generated catch block
        //     e1.printStackTrace();
        // }



        // ArrayList<List<String>> pttt = new ArrayList<>();
        // pttt.add(Arrays.asList("hello", "hi"));
        // pttt.add(Arrays.asList("hello"));
        // pttt.add(Arrays.asList("hello"));

        // System.out.println(pttt.stream().mapToInt(i -> i.size()).sum());


        // String test = "hello  asd asd   asd asd";
        // var arguments = test.split(" +");
        // for (String p : arguments) {
        //     System.out.println(p);
        // }





        // ConcurrentHashMap<Integer, String> hmap = new ConcurrentHashMap<>();
        // hmap.put(123,"123");
        // hmap.put(1232,"123");
        // hmap.put(1234,"123");
        // hmap.put(1235,"123");
        // hmap.put(1236,"123");
        // hmap.put(12323,"123");
        // hmap.put(123123,"123");
        // hmap.put(1231231,"123");
        // hmap.put(1231231,"1223");

        // Map<String, List<Integer>> result = hmap.entrySet().stream().collect(Collectors.groupingBy(
        //     Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        
        // System.out.println(result);
        
        // HashMap<String, Integer> map = new HashMap<>();
        // map.put("1", 1);
        // map.put("1", 1);
        // map.put("1", 1);
        // map.put("2", 2);

        // System.out.println(map.toString());

        // HashMap<Integer, String> dStores = new HashMap<>();
        // dStores.put(123,"hello");

        // HashMap<Integer, String> ht = new HashMap<>();
        // ht.put(123,"hello");
        // ht.put(321, "penis");
        
        // List<Long> s = ht.entrySet().stream()
        // .map(f -> dStores.entrySet().stream()
        // .dropWhile(e -> !e.getValue().equals(f.getValue()))
        // .count()).collect(Collectors.toList());

        // System.out.println(s.toString());



        // new Worker().start();
        // new Worker().start();

        
        // try {
        //     Thread.sleep(300);
        // } catch (Exception e) {

        // }
        // clock.lock();
        // System.out.println("STARTED");

        // try {
        //     Thread.sleep(3000);
        //     clock.unlock();
        // } catch (InterruptedException e) {
        //     // TODO Auto-generated catch block
        //     e.printStackTrace();
        // }

        // System.out.println("DONE");
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
