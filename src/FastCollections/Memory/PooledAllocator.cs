using System;
using System.Collections.Generic;

namespace FastCollections.Memory
{
    public unsafe class PooledAllocator<T> : IAllocator<T>
    {
        public PooledAllocator(Func<int, IPool<T>> poolFactory)
        {
            _poolFactory = poolFactory;
        }

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

        public void Deallocate(T item, int size)
        {
            _pools[size].Free(item);
        }

        public void Dispose()
        {
            foreach (var pool in _pools.Values)
                pool.Dispose();
        }

        private Dictionary<int, IPool<T>> _pools = new Dictionary<int, IPool<T>>();
        private Func<int, IPool<T>> _poolFactory;
    }
}
