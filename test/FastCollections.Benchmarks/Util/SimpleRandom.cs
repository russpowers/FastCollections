using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FastCollections.Benchmarks.Util
{
    /// <summary>
    /// This is used for comparison with the C++ version
    /// </summary>
    public class SimpleRandom
    {
        public uint Next()
        {
            _seed = (1103515245 * _seed + 12345) % (0xFFFFFFFF);
            return _seed;
        }

        public uint Next(uint max)
        {
            return Next() % max;
        }

        private uint _seed = 123456789;
    }
}
