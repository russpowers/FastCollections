using System;
using System.Collections.Generic;

namespace FastCollections
{
    public struct DefaultKeyComparer<TKey> : IKeyComparer<TKey>
         where TKey : IComparable<TKey>
    {
        public bool Equals(TKey a, TKey b) => a.CompareTo(b) == 0;
        public bool GreaterThan(TKey a, TKey b) => a.CompareTo(b) == 1;
        public bool LessThan(TKey a, TKey b) => a.CompareTo(b) == -1;
    }
}
