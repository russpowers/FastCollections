using BenchmarkDotNet.Attributes;
using FastCollections.Unsafe;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FastCollections.Benchmarks.Unsafe
{
    public class BTreeBenchmarks
    {
        struct IntKeyPolicy : IKeyComparer<int>
        {
            public bool Equals(int a, int b) => a == b;
            public bool LessThan(int a, int b) => a < b;
            public bool GreaterThan(int a, int b) => a > b;
        }

        struct ULongKeyPolicy : IKeyComparer<ulong>
        {
            public bool Equals(ulong a, ulong b) => a == b;
            public bool LessThan(ulong a, ulong b) => a < b;
            public bool GreaterThan(ulong a, ulong b) => a > b;
        }

        struct BTreeConfig : IBTreeConfig
        {
            public int TargetSize => 336;
        }

        const int COUNT = 10000000;

        public BTreeBenchmarks()
        {
            Console.WriteLine("CTOR");
            values = new int[COUNT];
            for (int i = 0; i < COUNT; ++i)
                values[i] = i;

            values.Shuffle(10);
        }

        BTree<ulong, int> tree;

        int[] values;

        [Setup]
        public void Setup()
        {
            Console.WriteLine("SETUP");
            tree = new BTree<ulong, int>();
        }

        [Cleanup]
        public void Cleanup()
        {
            Console.WriteLine("CLEANUP");
            tree.Dispose();
        }

        [Benchmark(OperationsPerInvoke = 1)]
        public void Insert10MRandom()
        {
            Console.WriteLine("Insert10MRandom");
            for (int i = 0; i < COUNT; ++i)
                tree.Add((uint)values[i], values[i]);
        }
    }
}
