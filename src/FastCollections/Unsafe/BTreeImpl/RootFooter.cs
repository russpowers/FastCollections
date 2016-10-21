namespace FastCollections.Unsafe.BTreeImpl
{
    unsafe struct RootFooter
    {
        public NodeHeader* Rightmost;
        public ulong Size;
    }
}
