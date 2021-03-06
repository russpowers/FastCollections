﻿using FastCollections.Benchmarks;
using FastCollections.Benchmarks.Unsafe;
using FastCollections.Unsafe;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace FastCollections.SimpleBenchmark
{
    /// <summary>
    /// Just a simple benchmark to run quickly on the command line
    /// </summary>
    public class Program
    {
        struct BTreeConfig : IBTreeConfig
        {
            public int TargetSize => 336;
        }

        const int COUNT = 10000000;
        const int TESTCOUNT = 5;

        static BTree<ulong, int> tree = new BTree<ulong, int>();

        private static void Test(int[] values)
        {
            for (int i = 0; i < COUNT; ++i)
                tree.Add((uint)values[i], values[i]);
        }

        public static void Main(string[] args)
        {
            var values = new int[COUNT];
            for (int i = 0; i < COUNT; ++i)
                values[i] = i;

            values.Shuffle(10);
            var s = new Stopwatch();
            s.Start();
            for (int i = 0; i < TESTCOUNT; ++i)
            {
                Test(values);
                s.Stop();
                tree.Clear();
                s.Start();
            }
            Console.WriteLine(s.Elapsed.TotalSeconds / TESTCOUNT);
        }
    }
}
