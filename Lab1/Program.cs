using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Порівняння однопотокової та багатопотокових реалізацій обчислення суми
/// елементів масиву на C# (.NET 8) — 6 реалізацій.
///
/// Запуск:
///   dotnet run -c Release -- [size] [parts]
///
/// size  - кількість елементів масиву (за умовою задачі >= 1_000_000_000)
/// parts - на скільки частин ділити масив (кількість потоків/задач)
/// </summary>
class ArraySumBenchmark
{
    // ------------------------------------------------------------------
    // 0. Базова (еталонна) однопотокова реалізація
    // ------------------------------------------------------------------
    static long SingleThreadSum(int[] array)
    {
        long total = 0L;
        for (int i = 0; i < array.Length; i++) total += array[i];
        return total;
    }

    // ------------------------------------------------------------------
    // 1. "Голі" потоки System.Threading.Thread + ручний Join
    // ------------------------------------------------------------------
    static long ManualThreadsSum(int[] array, int parts)
    {
        int n = array.Length;
        int chunk = (n + parts - 1) / parts;
        var threads = new Thread[parts];
        var partial = new long[parts];

        for (int p = 0; p < parts; p++)
        {
            int from = p * chunk;
            int to = Math.Min(n, from + chunk);
            int idx = p;
            threads[p] = new Thread(() =>
            {
                long s = 0L;
                for (int i = from; i < to; i++) s += array[i];
                partial[idx] = s;
            });
            threads[p].Start();
        }
        foreach (var t in threads) t.Join(); // синхронне очікування завершення кожного потоку

        long total = 0L;
        foreach (var s in partial) total += s;
        return total;
    }

    // ------------------------------------------------------------------
    // 2. Task.Run + Task.WhenAll (Task Parallel Library)
    // ------------------------------------------------------------------
    static long TaskBasedSum(int[] array, int parts)
    {
        int n = array.Length;
        int chunk = (n + parts - 1) / parts;
        var tasks = new Task<long>[parts];

        for (int p = 0; p < parts; p++)
        {
            int from = p * chunk;
            int to = Math.Min(n, from + chunk);
            tasks[p] = Task.Run(() =>
            {
                long s = 0L;
                for (int i = from; i < to; i++) s += array[i];
                return s;
            });
        }
        Task.WaitAll(tasks); // синхронна точка об'єднання
        return tasks.Sum(t => t.Result);
    }

    // ------------------------------------------------------------------
    // 3. Parallel.For з ручним поділом та локальним акумулятором на потік
    //    (уникає false sharing завдяки localInit/localFinally)
    // ------------------------------------------------------------------
    static long ParallelForSum(int[] array, int parts)
    {
        object lockObj = new object();
        long total = 0L;

        Parallel.For<long>(
            0, parts,
            new ParallelOptions { MaxDegreeOfParallelism = parts },
            () => 0L, // локальний ініціалізатор для кожного потоку
            (p, state, localSum) =>
            {
                int n = array.Length;
                int chunk = (n + parts - 1) / parts;
                int from = (int)p * chunk;
                int to = Math.Min(n, from + chunk);
                for (int i = from; i < to; i++) localSum += array[i];
                return localSum;
            },
            localSum => { lock (lockObj) { total += localSum; } } // синхронне об'єднання
        );
        return total;
    }

    // ------------------------------------------------------------------
    // 4. PLINQ ("просунутий" декларативний засіб)
    // ------------------------------------------------------------------
    static long PlinqSum(int[] array, int parts)
    {
        return array.AsParallel()
                     .WithDegreeOfParallelism(parts)
                     .Sum(x => (long)x);
    }

    // ------------------------------------------------------------------
    // 5. Parallel.ForEach + Partitioner.Create (range-партиціювання,
    //    "просунутий" засіб — мінімізує накладні витрати диспетчеризації)
    // ------------------------------------------------------------------
    static long PartitionerSum(int[] array, int parts)
    {
        long total = 0L;
        var rangePartitioner = Partitioner.Create(0, array.Length,
            Math.Max(1, array.Length / parts));

        Parallel.ForEach(
            rangePartitioner,
            new ParallelOptions { MaxDegreeOfParallelism = parts },
            range =>
            {
                long localSum = 0L;
                for (int i = range.Item1; i < range.Item2; i++) localSum += array[i];
                Interlocked.Add(ref total, localSum); // атомарне синхронне об'єднання
            });
        return total;
    }

    // ------------------------------------------------------------------
    // Допоміжне: запуск і вимірювання часу
    // ------------------------------------------------------------------
    static void Run(string name, Func<long> fn, double baselineMs)
    {
        var sw = Stopwatch.StartNew();
        long result = fn();
        sw.Stop();
        double ms = sw.Elapsed.TotalMilliseconds;
        string speedup = baselineMs > 0 ? $"{baselineMs / ms:F2}x" : "-";
        Console.WriteLine($"{name,-28} | result={result,-14} | time={ms,10:F2} ms | speedup={speedup}");
    }

    static void Main(string[] args)
    {
        int size = args.Length > 0 ? int.Parse(args[0]) : 1_000_000_000;
        int parts = args.Length > 1 ? int.Parse(args[1]) : Environment.ProcessorCount;

        Console.WriteLine($"Розмір масиву: {size}");
        Console.WriteLine($"Кількість частин/потоків: {parts}");
        Console.WriteLine($"Доступно ядер (ProcessorCount): {Environment.ProcessorCount}");
        Console.WriteLine();

        Console.WriteLine("Генерація масиву...");
        var array = new int[size];
        var rnd = new Random(42);
        for (int i = 0; i < size; i++) array[i] = rnd.Next(100);
        Console.WriteLine("Готово.\n");

        var sw = Stopwatch.StartNew();
        long baselineResult = SingleThreadSum(array);
        sw.Stop();
        double baselineMs = sw.Elapsed.TotalMilliseconds;
        Console.WriteLine($"{"0. Single-thread (baseline)",-28} | result={baselineResult,-14} | time={baselineMs,10:F2} ms | speedup=1.00x");

        Run("1. Manual Thread + Join", () => ManualThreadsSum(array, parts), baselineMs);
        Run("2. Task.Run + WhenAll", () => TaskBasedSum(array, parts), baselineMs);
        Run("3. Parallel.For (local acc.)", () => ParallelForSum(array, parts), baselineMs);
        Run("4. PLINQ", () => PlinqSum(array, parts), baselineMs);
        Run("5. Partitioner + ForEach", () => PartitionerSum(array, parts), baselineMs);
    }
}
