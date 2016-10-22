using System;

namespace FastCollections.Memory
{
    /// <summary>
    /// Generic pool interface.
    /// </summary>
    /// <typeparam name="T">The type that is pooled.</typeparam>
    public interface IPool<T> : IDisposable
    {
        /// <summary>
        /// Gets a fresh item of type <typeparamref name="T"/> from the pool.  This may be a newly 
        /// allocated item, or it may be reused from a previous <see cref="Free(T)"/>.
        /// </summary>
        /// <returns>A fresh <typeparamref name="T"/> item.</returns>
        T Get();

        /// <summary>
        /// Frees a previously allocated item.  The item will be added back to the pool to be used
        /// later.
        /// </summary>
        /// <param name="item">The item to free.</param>
        void Free(T item);
    }
}
