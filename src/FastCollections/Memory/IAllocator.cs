namespace FastCollections.Memory
{
    public interface IAllocator<T>
    {
        T Allocate(int size);
        void Deallocate(T item, int size);
    }
}
