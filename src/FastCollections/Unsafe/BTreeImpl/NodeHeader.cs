namespace FastCollections.Unsafe.BTreeImpl
{
    unsafe struct NodeHeader
    {
        public bool IsLeaf;
        public byte Position;
        public byte MaxCount;
        public byte Count;
        public NodeHeader* Parent;
    }
}
