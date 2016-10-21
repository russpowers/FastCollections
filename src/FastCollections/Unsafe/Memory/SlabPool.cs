using FastCollections.Memory;
using System;
using System.Runtime.InteropServices;

namespace FastCollections.Unsafe.Memory
{
    public unsafe class SlabPool : IPool<IntPtr>
    {
        public static Func<int, SlabPool> Factory(int itemsPerSlab = 1024, int maxSlabSize = 0, int initialSlabs = 1)
        {
            return (itemSize) =>
            {
                var adjustedSlabSize = maxSlabSize > 0 ? Math.Min(itemsPerSlab * itemSize, maxSlabSize) : (itemSize * itemsPerSlab);
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
