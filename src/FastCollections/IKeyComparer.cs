namespace FastCollections
{

    /// <summary>
    /// And interface for the operations needed to compare keys in a sorted data structure.
    /// Why use this at all?  It generates *slightly* faster code than <see cref="System.Collections.Generic.IComparer{T}"/>,
    /// since it has one less branch.  Sometimes it does force the CLR to generate better
    /// surrounding code, though, since it uses less registers.
    /// 
    /// This is only to be used if you are really concerned about performance.  In most cases, the
    /// <see cref="DefaultKeyComparer{TKey}"/> is an excellent substitute.
    /// </summary>
    /// <typeparam name="TKey">The key type to compare.</typeparam>
    public interface IKeyComparer<TKey>
    {
        /// <summary>
        /// Returns true if the keys are equal.
        /// </summary>
        bool Equals(TKey a, TKey b);

        /// <summary>
        /// Returns true if the first key is greater than the second.
        /// </summary>
        bool GreaterThan(TKey a, TKey b);

        /// <summary>
        /// Returns true if the first key is less than the second.
        /// </summary>
        bool LessThan(TKey a, TKey b);
    }
}
