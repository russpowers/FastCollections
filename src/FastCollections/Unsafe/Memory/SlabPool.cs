using FastCollections.Memory;
using System;
using System.Runtime.InteropServices;

namespace FastCollections.Unsafe.Memory
{
    /// <summary>
    /// Allocates slabs of memory of a specified size, then uses that memory to allocate
    /// pooled items.
    /// </summary>
    public unsafe class SlabPool : IPool<IntPtr>
    {
        /// <summary>
        /// Produces a slab pool factory, used in <see cref="PooledAllocator{T}"/>.  If both <paramref name="maxSlabSize"/> and
        /// <paramref name="maxItemsPerSlab"/> are specified, maxSlabSize wins.
        /// </summary>
        /// <param name="maxItemsPerSlab">The maximum number of items per slab.</param>
        /// <param name="maxSlabSize">The maximum slab size.</param>
        /// <param name="initialSlabs">The initiual slab count.</param>
        /// <returns></returns>
        public static Func<int, SlabPool> Factory(int maxItemsPerSlab = 1024, int maxSlabSize = 0, int initialSlabs = 1)
        {
            return (itemSize) =>
            {
                var adjustedSlabSize = maxSlabSize > 0 ? Math.Min(maxItemsPerSlab * itemSize, maxSlabSize) : (itemSize * maxItemsPerSlab);
                var adjustedItemsPerSlab = adjustedSlabSize / itemSize;
                return new SlabPool(itemSize, adjustedItemsPerSlab, initialSlabs);
            };
        }

        private struct Slab
        {
            public Slab(IntPtr ptr)
            {
                Ptr = (byte*)ptr;
                NextIndex = 0;
            }

            public readonly byte* Ptr;
            public int NextIndex;
        }

        /// <summary>
        /// Create a new slab pool.
        /// </summary>
        /// <param name="itemSize">The size of the items.</param>
        /// <param name="itemsPerSlab">The number of items per slab.</param>
        /// <param name="initialSlabs">The initiual slab count.</param>
        public SlabPool(int itemSize, int itemsPerSlab = 1024, int initialSlabs = 1)
        {
            _itemSize = itemSize;
            _itemsPerSlab = itemsPerSlab;
            _slabs = new Slab[initialSlabs];
            for (int i = 0; i < initialSlabs; ++i)
            {
                _slabs[i] = CreateSlab();
            }
        }

        /// <summary>
        /// Get a new pooled item.
        /// </summary>
        public IntPtr Get()
        {
            if (_freeListLength > 0)
            {
                _freeListLength -= 1;
                return (IntPtr)_freeList[_freeListLength];
            }
            else
            {
                if (_slabs[_currentSlab].NextIndex == _itemsPerSlab)
                {
                    if (_currentSlab == _slabs.Length - 1)
                    {
                        var newSlabs = new Slab[_slabs.Length * 2];
                        for (var i = 0; i < _slabs.Length; ++i)
                            newSlabs[i] = _slabs[i];
                        _slabs = newSlabs;
                        for (var i = _currentSlab + 1; i < _slabs.Length; ++i)
                            newSlabs[i] = CreateSlab();
                    }

                    _currentSlab += 1;
                }

                return (IntPtr)(_slabs[_currentSlab].Ptr + _itemSize * (_slabs[_currentSlab].NextIndex++));
            }
        }

        /// <summary>
        /// Free a previously allocated pooled item.
        /// </summary>
        public void Free(IntPtr item)
        {
            if (_freeList.Length <= _freeListLength)
            {
                var newFreeList = new byte*[_freeList.Length * 2];
                for (var i = 0; i < _freeList.Length; ++i)
                    newFreeList[i] = _freeList[i];
                _freeList = newFreeList;
            }

            _freeList[_freeListLength++] = (byte*)item;
        }

        /// <summary>
        /// Dispose of all slabs.
        /// </summary>
        public void Dispose()
        {
            foreach (var slab in _slabs)
                Marshal.FreeHGlobal((IntPtr)slab.Ptr);
        }

        private Slab CreateSlab()
        {
            return new Slab(Marshal.AllocHGlobal(_itemsPerSlab * _itemSize));
        }

        private int _itemSize;
        private int _itemsPerSlab;
        private int _freeListLength = 0;
        private byte*[] _freeList = new byte*[32];
        private int _currentSlab = 0;
        private Slab[] _slabs;
    }
}
