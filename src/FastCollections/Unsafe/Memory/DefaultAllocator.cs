using FastCollections.Memory;
using System;
using System.Runtime.InteropServices;

namespace FastCollections.Unsafe.Memory
{
    /// <summary>
    /// The default unsafe allocator that uses <see cref="Marshal.AllocHGlobal(int)" /> 
    /// and <see cref="Marshal.FreeHGlobal(IntPtr)"/> />.
    /// </summary>
    public class DefaultAllocator : IAllocator<IntPtr>
    {
        /// <summary>
        /// Allocate a new block of memory on the heap.
        /// </summary>
        /// <param name="size">The size of the block of memory.</param>
        /// <returns>A pointer to the block of memory.</returns>
        public IntPtr Allocate(int size)
        {
            return Marshal.AllocHGlobal(size);
        }

        /// <summary>
        /// Deallocate a new block of memory on the heap.
        /// </summary>
        /// <param name="ptr">The pointer to the block of memory to deallocate.</param>
        /// <param name="size">The size of the block of memory (not used).</param>
        public void Deallocate(IntPtr ptr, int size)
        {
            Marshal.FreeHGlobal(ptr);
        }

        /// <summary>
        /// Dispose (not used).
        /// </summary>
        public void Dispose()
        {
        }
    }
}
