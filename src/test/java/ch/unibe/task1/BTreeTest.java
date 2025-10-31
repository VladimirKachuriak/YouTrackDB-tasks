package ch.unibe.task1;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Here, tests are performed for {@link BTreeLocks} and BTreeLockFree. Here are two {@code putAndGetWitThread()} tests
 * that check the integrity of the tree structure during concurrent insertion. For this purpose, {@link ConcurrentSkipListMap}
 * is used, in which all operations are recorded and then compared with our implementations.
 */
class BTreeTest {

    /**
     * Simplified testing for basic imputation operation for one Thread
     */
    @Test
    void putAndGet() {
        Map<byte[], byte[]> map = Map.of(
                "aaa".getBytes(), "value1".getBytes(),
                "banana".getBytes(), "value2".getBytes(),
                "apple".getBytes(), "value3".getBytes(),
                "cat".getBytes(), "value14".getBytes(),
                "126755".getBytes(), "value5".getBytes()
        );

        BTreeLockFree bTree = new BTreeLockFree();

        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            bTree.put(entry.getKey(), entry.getValue());
        }


        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            assertEquals(bTree.get(entry.getKey()), entry.getValue());
        }
    }

    /**
     * performs consistency check of our BTree implementation
     */
    @Test
    void putAndGetWitThread() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        final int NUM_THREADS = 10;
        final int KEY_SIZE = 10;
        final int VALUE_SIZE = 10;
        final int NUM_ENTRIES = 100_000;

        List<Class<?>> classes = List.of(BTreeLocks.class, BTreeLockFree.class);
        for (Class<?> clazz : classes) {
            System.out.println("Now Runs: " + clazz.getSimpleName());
            MyBTree myBTree = (MyBTree) createInstance(clazz);

            Map<byte[], byte[]> map = new ConcurrentSkipListMap<>(Util::compareByteArrays);
            Thread[] threads = new Thread[NUM_THREADS];

            BTreeLocks bTree = new BTreeLocks();
            for (int i = 0; i < NUM_THREADS; i++) {
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < NUM_ENTRIES; j++) {
                        byte[] key = generateRandomByteArray(KEY_SIZE);
                        byte[] value = generateRandomByteArray(VALUE_SIZE);
                        map.put(key, value);
                        bTree.put(key, value);
                    }
                });
            }
            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }


            //Validation
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
//            System.out.printf("MyBTree Key: %s, Value: %s%n", Arrays.toString(entry.getKey()), Arrays.toString(bTree.get(entry.getKey())));
//            System.out.printf("Actual Key: %s, Value: %s%n%n", Arrays.toString(entry.getKey()), Arrays.toString(entry.getValue()));
                assertArrayEquals(entry.getValue(), bTree.get(entry.getKey()));
            }
        }
    }

    @Test
    void performanceTest() throws InterruptedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        final int KEY_SIZE = 10;
        final int VALUE_SIZE = 100;
        final int NUM_ENTRIES = 500_000;

        int[] threads_num = {1, 2, 4, 8, 16};
        List<Class<?>> classes = List.of(Collections.synchronizedMap(new TreeMap<>(Util::compareByteArrays)).getClass(),
                BTreeLocks.class, BTreeLockFree.class, ConcurrentSkipListMap.class, ConcurrentHashMap.class);
        List<String> results = new ArrayList<>();

        for (Class<?> clazz : classes) {
            System.out.println("Now runs" + ": " + clazz.getSimpleName());
            Object object = createInstance(clazz);
            for (int i = 0; i < threads_num.length; i++) {
                int THREAD_NUM = threads_num[i];
                Thread[] threads = new Thread[THREAD_NUM];


                for (int j = 0; j < THREAD_NUM; j++) {
                    threads[j] = new Thread(() -> {
                        for (int x = 0; x < NUM_ENTRIES; x++) {
                            byte[] key = generateRandomByteArray(KEY_SIZE);
                            byte[] value = generateRandomByteArray(VALUE_SIZE);

                            universalPut(key, value, object);
                        }
                    });
                }

                Instant start = Instant.now();
                for (Thread t : threads) {
                    t.start();
                }

                for (Thread t : threads) {
                    t.join();
                }
//            System.out.println(bTree.size());
                Instant finish = Instant.now();
                long timeElapsed = Duration.between(start, finish).toMillis();
                results.add(String.format(
                        Locale.US, "%s, opsPerThread: %s, T: %d, Elapsed: %d ms",
                        clazz.getSimpleName(), NUM_ENTRIES, THREAD_NUM, timeElapsed));
            }
            results.add("\n");
        }
        for (String result : results) {
            System.out.println(result);
        }
    }

    private Object createInstance(Class<?> clazz) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (clazz.equals(ConcurrentSkipListMap.class)) {
            Constructor<?> constructor = clazz.getDeclaredConstructor(Comparator.class);
            Comparator<byte[]> comparator = Util::compareByteArrays;
            return constructor.newInstance(comparator);
        }
        if (clazz.equals(Collections.synchronizedMap(new TreeMap<>()).getClass())) {
            Comparator<byte[]> comparator = Util::compareByteArrays;
            return Collections.synchronizedMap(new TreeMap<>(comparator));
        }

        return clazz.getDeclaredConstructor().newInstance();
    }

    private void universalPut(byte[] key, byte[] values, Object object) {
        switch (object) {
            case Map<?, ?> i -> ((Map<byte[], byte[]>) i).put(key, values);
            case BTreeLockFree i -> i.put(key, values);
            case BTreeLocks i -> i.put(key, values);
            default -> throw new IllegalArgumentException("Object not supported");
        }
    }

    private byte[] generateRandomByteArray(int maxLength) {
        Random r = new Random();
        int length = r.nextInt(1, maxLength + 1);
        byte[] array = new byte[length];

        r.nextBytes(array);
        return array;
    }

}