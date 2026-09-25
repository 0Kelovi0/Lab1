import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * Порівняння однопотокового та багатопотокових алгоритмів обчислення суми
 * елементів масиву на Java (7 реалізацій).
 *
 * Запуск:
 *   javac ArraySumBenchmark.java
 *   java -Xmx8g ArraySumBenchmark [size] [parts]
 *
 * size  - кількість елементів масиву (за умовою задачі >= 1_000_000_000)
 * parts - на скільки частин ділити масив (кількість потоків/задач)
 *
 * Примітка щодо пам'яті: int[1_000_000_000] займає ~4 ГБ. Для повноцінного
 * запуску потрібна машина з достатнім обсягом ОЗУ та прапорцем -Xmx.
 */
public class ArraySumBenchmark {

    // Сума елементів рахується в long, щоб уникнути переповнення int.
    interface SumStrategy {
        long sum(int[] array, int parts) throws Exception;
        String name();
    }

    // ------------------------------------------------------------------
    // 0. Базова (еталонна) однопотокова реалізація
    // ------------------------------------------------------------------
    static long singleThreadSum(int[] array) {
        long total = 0L;
        for (int value : array) {
            total += value;
        }
        return total;
    }

    // ------------------------------------------------------------------
    // 1. "Голі" потоки java.lang.Thread + ручний join
    // ------------------------------------------------------------------
    static long manualThreadsSum(int[] array, int parts) throws InterruptedException {
        int n = array.length;
        int chunk = (n + parts - 1) / parts;
        Thread[] threads = new Thread[parts];
        long[] partial = new long[parts];

        for (int p = 0; p < parts; p++) {
            final int from = p * chunk;
            final int to = Math.min(n, from + chunk);
            final int idx = p;
            threads[p] = new Thread(() -> {
                long s = 0L;
                for (int i = from; i < to; i++) {
                    s += array[i];
                }
                partial[idx] = s;
            }, "manual-thread-" + p);
            threads[p].start();
        }
        // синхронне об'єднання результатів: чекаємо завершення кожного потоку
        for (Thread t : threads) {
            t.join();
        }
        long total = 0L;
        for (long s : partial) total += s;
        return total;
    }

    // ------------------------------------------------------------------
    // 2. ExecutorService (fixed thread pool) + Callable/Future
    // ------------------------------------------------------------------
    static long executorServiceSum(int[] array, int parts) throws InterruptedException, ExecutionException {
        int n = array.length;
        int chunk = (n + parts - 1) / parts;
        ExecutorService pool = Executors.newFixedThreadPool(parts);
        try {
            List<Future<Long>> futures = new ArrayList<>(parts);
            for (int p = 0; p < parts; p++) {
                final int from = p * chunk;
                final int to = Math.min(n, from + chunk);
                futures.add(pool.submit(() -> {
                    long s = 0L;
                    for (int i = from; i < to; i++) s += array[i];
                    return s;
                }));
            }
            long total = 0L;
            for (Future<Long> f : futures) total += f.get(); // синхронне очікування результату
            return total;
        } finally {
            pool.shutdown();
        }
    }

    // ------------------------------------------------------------------
    // 3. CompletableFuture (асинхронний конвеєр + join у кінці)
    // ------------------------------------------------------------------
    static long completableFutureSum(int[] array, int parts) {
        int n = array.length;
        int chunk = (n + parts - 1) / parts;
        ExecutorService pool = Executors.newFixedThreadPool(parts);
        try {
            @SuppressWarnings("unchecked")
            CompletableFuture<Long>[] futures = new CompletableFuture[parts];
            for (int p = 0; p < parts; p++) {
                final int from = p * chunk;
                final int to = Math.min(n, from + chunk);
                futures[p] = CompletableFuture.supplyAsync(() -> {
                    long s = 0L;
                    for (int i = from; i < to; i++) s += array[i];
                    return s;
                }, pool);
            }
            // синхронна точка об'єднання
            CompletableFuture.allOf(futures).join();
            long total = 0L;
            for (CompletableFuture<Long> f : futures) total += f.join();
            return total;
        } finally {
            pool.shutdown();
        }
    }

    // ------------------------------------------------------------------
    // 4. Fork/Join Framework (work-stealing, "просунутий" засіб)
    // ------------------------------------------------------------------
    static class SumTask extends RecursiveTask<Long> {
        private final int[] array;
        private final int from, to;
        private final int threshold;

        SumTask(int[] array, int from, int to, int threshold) {
            this.array = array; this.from = from; this.to = to; this.threshold = threshold;
        }

        @Override
        protected Long compute() {
            if (to - from <= threshold) {
                long s = 0L;
                for (int i = from; i < to; i++) s += array[i];
                return s;
            }
            int mid = from + (to - from) / 2;
            SumTask left = new SumTask(array, from, mid, threshold);
            SumTask right = new SumTask(array, mid, to, threshold);
            left.fork();                 // асинхронно виконати ліву половину
            long rightResult = right.compute(); // праву - в поточному потоці
            long leftResult = left.join();       // синхронне очікування лівої
            return leftResult + rightResult;
        }
    }

    static long forkJoinSum(int[] array, int parts) {
        int threshold = Math.max(1, array.length / parts);
        ForkJoinPool pool = new ForkJoinPool(parts);
        try {
            return pool.invoke(new SumTask(array, 0, array.length, threshold));
        } finally {
            pool.shutdown();
        }
    }

    // ------------------------------------------------------------------
    // 5. Parallel Stream (декларативний "просунутий" засіб, ForkJoinPool.commonPool)
    // ------------------------------------------------------------------
    static long parallelStreamSum(int[] array, int parts) throws ExecutionException, InterruptedException {
        // Керуємо ступенем паралелізму через окремий ForkJoinPool із заданою кількістю потоків (=parts).
        ForkJoinPool customPool = new ForkJoinPool(parts);
        try {
            return customPool.submit(() ->
                    Arrays.stream(array).parallel().asLongStream().sum()
            ).get();
        } finally {
            customPool.shutdown();
        }
    }

    // ------------------------------------------------------------------
    // 6. Virtual Threads (Java 21, Project Loom)
    // ------------------------------------------------------------------
    static long virtualThreadsSum(int[] array, int parts) throws InterruptedException, ExecutionException {
        int n = array.length;
        int chunk = (n + parts - 1) / parts;
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Long>> futures = new ArrayList<>(parts);
            for (int p = 0; p < parts; p++) {
                final int from = p * chunk;
                final int to = Math.min(n, from + chunk);
                futures.add(pool.submit(() -> {
                    long s = 0L;
                    for (int i = from; i < to; i++) s += array[i];
                    return s;
                }));
            }
            long total = 0L;
            for (Future<Long> f : futures) total += f.get();
            return total;
        }
    }

    // ------------------------------------------------------------------
    // Допоміжне: запуск і вимірювання часу
    // ------------------------------------------------------------------
    static void run(String name, int[] array, int parts, ThrowingLongSupplier fn, long baseline) throws Exception {
        long t0 = System.nanoTime();
        long result = fn.get();
        long t1 = System.nanoTime();
        double ms = (t1 - t0) / 1_000_000.0;
        String speedup = baseline > 0 ? String.format("%.2fx", baseline / (double) (t1 - t0)) : "-";
        System.out.printf("%-28s | result=%-14d | time=%10.2f ms | speedup=%s%n", name, result, ms, speedup);
    }

    interface ThrowingLongSupplier { long get() throws Exception; }

    public static void main(String[] args) throws Exception {
        int size = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000_000;
        int parts = args.length > 1 ? Integer.parseInt(args[1]) : Runtime.getRuntime().availableProcessors();

        System.out.println("Розмір масиву: " + size);
        System.out.println("Кількість частин/потоків: " + parts);
        System.out.println("Доступно ядер (availableProcessors): " + Runtime.getRuntime().availableProcessors());
        System.out.println();

        System.out.println("Генерація масиву...");
        int[] array = new int[size];
        Random rnd = new Random(42);
        for (int i = 0; i < size; i++) array[i] = rnd.nextInt(100); // невеликі значення, щоб контролювати діапазон суми
        System.out.println("Готово.\n");

        long t0 = System.nanoTime();
        long baselineResult = singleThreadSum(array);
        long t1 = System.nanoTime();
        long baselineNanos = t1 - t0;
        System.out.printf("%-28s | result=%-14d | time=%10.2f ms | speedup=%s%n",
                "0. Single-thread (baseline)", baselineResult, baselineNanos / 1_000_000.0, "1.00x");

        run("1. Manual Thread + join", array, parts, () -> manualThreadsSum(array, parts), baselineNanos);
        run("2. ExecutorService+Future", array, parts, () -> executorServiceSum(array, parts), baselineNanos);
        run("3. CompletableFuture", array, parts, () -> completableFutureSum(array, parts), baselineNanos);
        run("4. ForkJoinPool (RecursiveTask)", array, parts, () -> forkJoinSum(array, parts), baselineNanos);
        run("5. Parallel Stream", array, parts, () -> parallelStreamSum(array, parts), baselineNanos);
        run("6. Virtual Threads", array, parts, () -> virtualThreadsSum(array, parts), baselineNanos);
    }
}
