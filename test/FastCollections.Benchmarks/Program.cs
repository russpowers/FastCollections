using BenchmarkDotNet.Running;
using FastCollections.Benchmarks.Unsafe;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FastCollections.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkRunner.Run<BTreeBenchmarks>();
        }
    }
}
