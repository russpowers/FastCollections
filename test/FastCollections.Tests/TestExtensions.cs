using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FastCollections.Tests
{
    public static class TestExtensions
    {
        static Random rand = new Random();

        public static void Shuffle<T>(this IList<T> list, int times = 1)
        {
            for (int i = 0; i < times; ++i)
            {
                int n = list.Count;
                while (n > 1)
                {
                    n--;
                    int k = rand.Next(n + 1);
                    T value = list[k];
                    list[k] = list[n];
                    list[n] = value;
                }
            }
        }
    }
}
