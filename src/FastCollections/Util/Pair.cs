namespace FastCollections.Util
{
    public struct Pair<T, U>
    {
        public Pair(T first, U second)
        {
            First = first;
            Second = second;
        }

        public readonly T First;
        public readonly U Second;
    }
}
