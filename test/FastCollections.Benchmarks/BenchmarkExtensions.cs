using FastCollections.Benchmarks.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FastCollections.Benchmarks
{
    public static class BenchmarkExtensions
    {
        private static SimpleRandom rand = new SimpleRandom();

        public static void Shuffle<T>(this IList<T> list, int times = 1)
        {
            for (int i = 0; i < times; ++i)
            {
                int n = list.Count;
                while (n > 1)
                {
                    n--;
                    int k = (int)rand.Next((uint)n + 1);
                    T value = list[k];
                    list[k] = list[n];
                    list[n] = value;
                }
            }
        }
    }
}
