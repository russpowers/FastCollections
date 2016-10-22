using System;
using System.Collections.Generic;

namespace FastCollections
{
    /// <summary>
    /// Default key comparer using <see cref="IComparable{T}"/>.
    /// </summary>
    /// <typeparam name="TKey">The key type to compare.</typeparam>
    public struct DefaultKeyComparer<TKey> : IKeyComparer<TKey>
         where TKey : IComparable<TKey>
    {
        /// <summary>
        /// Returns true if the keys are equal.
        /// </summary>
        public bool Equals(TKey a, TKey b) => a.CompareTo(b) == 0;

        /// <summary>
        /// Returns true if the first key is greater than the second.
        /// </summary>
        public bool GreaterThan(TKey a, TKey b) => a.CompareTo(b) == 1;

        /// <summary>
        /// Returns true if the first key is less than the second.
        /// </summary>
        public bool LessThan(TKey a, TKey b) => a.CompareTo(b) == -1;
    }
}
