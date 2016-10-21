using FastCollections.Memory;
using System;
using System.Runtime.InteropServices;

namespace FastCollections.Unsafe.Memory
{
    public class DefaultAllocator : IAllocator<IntPtr>
    {
        public IntPtr Allocate(int size)
        {
            return Marshal.AllocHGlobal(size);
        }

        public void Deallocate(IntPtr ptr, int size)
        {
            Marshal.FreeHGlobal(ptr);
        }

        public void Dispose()
        {
        }

        private IntPtr _lastMem = IntPtr.Zero;
    }
}
