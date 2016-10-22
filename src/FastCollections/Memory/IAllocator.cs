namespace FastCollections.Memory
{
    /// <summary>
    /// Generic allocator interface.
    /// </summary>
    /// <typeparam name="T">The type to allocate.</typeparam>
    public interface IAllocator<T>
    {
        /// <summary>
        /// Allocates a give item of type <typeparamref name="T"/> with size <paramref name="size"/>.
        /// </summary>
        /// <param name="size">The size of the allocate item.</param>
        /// <returns></returns>
        T Allocate(int size);

        /// <summary>
        /// Deallocates a give item of type <typeparamref name="T"/> with size <paramref name="size"/>.
        /// </summary>
        /// <param name="item">The previously allocated item.</param>
        /// <param name="size">The size of the item.</param>
        /// <returns></returns>
        void Deallocate(T item, int size);
    }
}
