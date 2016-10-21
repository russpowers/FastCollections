using System;

namespace FastCollections.Memory
{
    public interface IPool<T> : IDisposable
    {
        T Get();
        void Free(T item);
    }
}
