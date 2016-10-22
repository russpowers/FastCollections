using System;
using System.Collections.Generic;

namespace FastCollections.Memory
{
    /// <summary>
    /// Allocates items using multipe fixed-size pools.
    /// </summary>
    /// <typeparam name="T">The type to be pooled.</typeparam>
    public unsafe class PooledAllocator<T> : IAllocator<T>
    {
        /// <summary>
        /// Create new <see cref="PooledAllocator{T}"/>.
        /// </summary>
        /// <param name="poolFactory">A factory for creating pools taking an item size
        /// and returning a pool.
        /// </param>
        public PooledAllocator(Func<int, IPool<T>> poolFactory)
        {
            _poolFactory = poolFactory;
        }

        /// <summary>
        /// Allocates a new item from one of the pools.
        /// </summary>
        /// <param name="size">The size to allocate.</param>
        /// <returns>A fresh item.</returns>
        public T Allocate(int size)
        {
            IPool<T> pool;
            if (!_pools.TryGetValue(size, out pool))
            {
                pool = _poolFactory(size);
                _pools.Add(size, pool);
            }
            return pool.Get();
        }

        /// <summary>
        /// Deallocate an item, which is freed in the pools.
        /// </summary>
        /// <param name="item">The item to deallocate.</param>
        /// <param name="size">The size of the item.</param>
        public void Deallocate(T item, int size)
        {
            _pools[size].Free(item);
        }

        /// <summary>
        /// Free any allocated pools and their items.
        /// </summary>
        public void Dispose()
        {
            foreach (var pool in _pools.Values)
                pool.Dispose();
        }

        private Dictionary<int, IPool<T>> _pools = new Dictionary<int, IPool<T>>();
        private Func<int, IPool<T>> _poolFactory;
    }
}
