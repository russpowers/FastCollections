// Copyright 2016 Russell Powers
//
// This is based on the Google BTree C++ library.  The original code has
// been heavily modified to work in C#.
//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Collections;
using MemUnsafe = DotNetCross.Memory.Unsafe;
using FastCollections.Unsafe.BTreeImpl;
using FastCollections.Util;
using FastCollections.Memory;
using FastCollections.Unsafe.Memory;

using CounterType = System.Byte;
using SizeType = System.UInt64;

namespace FastCollections.Unsafe
{
    // I know that it's kind of gross to put everything in one huge file, but it would be messy to remove
    // something big like the internal Node struct.. I will revisit the idea later when I have more time.

    public interface IBTreeConfig
    {
        int TargetSize { get; }
    }

    public struct DefaultBTreeConfig : IBTreeConfig
    {
        public int TargetSize => 256;
    }

    /// <summary>
    /// A high-performance, space-efficient B-tree implementation using unmanaged memory.
    /// </summary>
    /// <typeparam name="TKey">The key type.  MUST NOT CONTAIN ANY MANAGED OBJECTS.</typeparam>
    /// <typeparam name="TValue">Type value type.  MUST NOT CONTAIN ANY MANAGED OBJECTS.</typeparam>
    /// <typeparam name="TKeyComparer">The custom key comparer type used to sort keys.</typeparam>
    /// <typeparam name="TConfig">The configuration type used to set custom B-tree parameters.</typeparam>
    public unsafe class BTree<TKey, TValue, TKeyComparer, TConfig> : IDictionary<TKey, TValue>, IDisposable
        where TKey : struct
        where TValue : struct
        where TKeyComparer : struct, IKeyComparer<TKey>
        where TConfig : struct, IBTreeConfig
    {
        private static readonly TConfig _config = default(TConfig);
        private static readonly EqualityComparer<TValue> _valueEqComparer = EqualityComparer<TValue>.Default;
        private static TKeyComparer _keyComparer = default(TKeyComparer);

        // These get used to calculate memory allocation sizes and offsets within the nodes.
        // The new x64 .NET compiler (RyuJIT) actually manages to inline these as constants in
        // the generated assembly code.  Yay!
        private static readonly int TARGET_NODE_SIZE = _config.TargetSize;
        private static readonly int SIZEOF_PTR = sizeof(void*);
        private static readonly uint USIZEOF_PTR = (uint)sizeof(void*);
        private static readonly int SIZEOF_SIZE_TYPE = sizeof(SizeType);
        private static readonly int SIZEOF_KEY = MemUnsafe.SizeOf<TKey>();
        private static readonly int SIZEOF_VALUE = MemUnsafe.SizeOf<TValue>();
        private static readonly int SIZEOF_KEY_VALUE = MemUnsafe.SizeOf<KeyValuePair<TKey, TValue>>();
        private static readonly ulong USIZEOF_KEY_VALUE = (uint)SIZEOF_KEY_VALUE;
        private static readonly int NODE_VALUE_SPACE = TARGET_NODE_SIZE - 2 * SIZEOF_PTR;
        private static readonly int SIZEOF_BASE_HEADER = MemUnsafe.SizeOf<NodeHeader>();
        private static readonly int NODE_TARGET_VALUES = (TARGET_NODE_SIZE - SIZEOF_BASE_HEADER) / SIZEOF_KEY_VALUE;
        private static readonly int NODE_KV_COUNT = NODE_TARGET_VALUES >= 3 ? NODE_TARGET_VALUES : 3;
        private static readonly int MIN_NODE_KV_COUNT = NODE_KV_COUNT / 2;
        private static readonly int SIZEOF_NODE_KVS = NODE_KV_COUNT * SIZEOF_KEY_VALUE;
        private static readonly int NODE_CHILDREN_COUNT = NODE_KV_COUNT + 1;
        private static readonly int SIZEOF_NODE_CHILDREN = NODE_CHILDREN_COUNT * SIZEOF_PTR;
        private static readonly int SIZEOF_LEAF_NODE = SIZEOF_BASE_HEADER + SIZEOF_NODE_KVS;
        private static readonly int SIZEOF_INTERNAL_NODE = SIZEOF_LEAF_NODE + SIZEOF_NODE_CHILDREN;
        private static readonly int SIZEOF_ROOT_NODE = SIZEOF_INTERNAL_NODE + SIZEOF_PTR + SIZEOF_SIZE_TYPE;
        private static readonly ulong CHILDREN_OFFSET = (uint)SIZEOF_BASE_HEADER + (uint)SIZEOF_NODE_KVS;
        private static readonly int FOOTER_OFFSET = SIZEOF_BASE_HEADER + SIZEOF_NODE_KVS + SIZEOF_NODE_CHILDREN;

        // The below are used to try to force inlining.  It works most of the time with x64.

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool KeyEquals(TKey a, TKey b) => _keyComparer.Equals(a, b);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool KeyLessThan(TKey a, TKey b) => _keyComparer.LessThan(a, b);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ValueEquals(TValue a, TValue b) => _valueEqComparer.Equals(a, b);

        /// <summary>
        /// The iterator is used to store positions and in algoritms on the B-tree.
        /// This is left over from the C++ version, and it seems to work well, so I've left it.
        /// It is only used internally, however, and only enumerables are exposed.
        /// </summary>
        public struct Iterator
        {
            public static bool operator ==(Iterator a, Iterator b) => a.Node == b.Node && a.Position == b.Position;
            public static bool operator !=(Iterator a, Iterator b) => !(a == b);

            public override bool Equals(object obj)
            {
                if (obj is Iterator)
                    return ((Iterator)obj) == this;
                else
                    return false;
            }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public static Iterator operator ++(Iterator iterator)
            {
                iterator.Increment();
                return iterator;
            }

            public static Iterator operator --(Iterator iterator)
            {
                iterator.Decrement();
                return iterator;
            }

            public static implicit operator bool(Iterator iterator) => iterator.IsValid;

            internal Iterator(Node node, int position)
            {
                Node = node;
                Position = position;
            }

            /// <summary>
            /// Move to the next sorted element in the B-tree.
            /// </summary>
            public void Increment()
            {
                if (Node.IsLeaf && ++Position < Node.Count)
                    return;
                IncrementSlow();
            }

            private void IncrementBy(int count)
            {
                while (count > 0)
                {
                    if (Node.IsLeaf)
                    {
                        int rest = Node.Count - Position;
                        Position += Math.Min(rest, count);
                        count = count - rest;
                        if (Position < Node.Count)
                        {
                            return;
                        }
                    }
                    else
                    {
                        --count;
                    }
                    IncrementSlow();
                }
            }

            private void IncrementSlow()
            {
                if (Node.IsLeaf)
                {
                    Debug.Assert(Position >= Node.Count);
                    Iterator save = this;
                    while (Position == Node.Count && !Node.IsRoot)
                    {
                        Debug.Assert(Node.Parent.Child(Node.Position) == Node);
                        Position = Node.Position;
                        Node = Node.Parent;
                    }
                    if (Position == Node.Count)
                    {
                        this = save;
                    }
                }
                else
                {
                    Debug.Assert(Position < Node.Count);
                    Node = Node.Child(Position + 1);
                    while (!Node.IsLeaf)
                    {
                        Node = Node.Child(0);
                    }
                    Position = 0;
                }
            }

            /// <summary>
            /// Move to the previous sorted element in the B-tree.
            /// </summary>
            public void Decrement()
            {
                if (Node.IsLeaf && --Position >= 0)
                    return;
                DecrementSlow();
            }

            private void DecrementSlow()
            {
                if (Node.IsLeaf)
                {
                    Debug.Assert(Position <= -1);
                    Iterator save = this;
                    while (Position < 0 && !Node.IsRoot)
                    {
                        Debug.Assert(Node.Parent.Child(Node.Position) == Node);
                        Position = Node.Position - 1;
                        Node = Node.Parent;
                    }
                    if (Position < 0)
                    {
                        this = save;
                    }
                }
                else
                {
                    Debug.Assert(Position >= 0);
                    Node = Node.Child(Position);
                    while (!Node.IsLeaf)
                    {
                        Node = Node.Child(Node.Count);
                    }
                    Position = Node.Count - 1;
                }
            }

            /// <summary>
            /// Returns true if the iterator is valid and has a key/value associated with it.
            /// </summary>
            public bool IsValid => Node.NonEmpty && (uint)Position < Node.CountUInt;

            /// <summary>
            /// Gets the key associated with the position of this iterator.  For performance, iterator validity is 
            /// not checked, and the result is undefined for an invalid iterator.
            /// </summary>
            public TKey Key => Node.Key(Position);

            /// <summary>
            /// Gets or sets the value associated with the key of this iterator.  This checks if the iterator is
            /// invalid when setting a value, and will throw a <see cref="NullReferenceException"/>.  For performance,
            /// iterator validity is not checked on the getter, and the result is undefined for an invalid iterator.
            /// </summary>
            public TValue Value
            {
                get
                {
                    return Node.Value(Position);
                }
                set
                {
                    if ((uint)Position < Node.CountUInt)
                        Node.SetValue(Position, value);
                    else
                        throw new NullReferenceException();
                }
            }

            /// Sets the value associated with the key of this iterator. Throws <see cref="NullReferenceException"/> 
            /// if the iterator is invalid.
            /// </summary>
            /// <param name="value">The new value.</param>
            public void SetValue(TValue value)
            {
                if ((uint)Position < Node.CountUInt)
                    Node.SetValue(Position, value);
                else
                    throw new NullReferenceException();
            }

            /// Gets the element associated with the key of this iterator.  For performance, iterator validity is 
            /// not checked, and the result is undefined for an invalid iterator.
            public KeyValuePair<TKey, TValue> KeyValue => Node.GetKeyValue(Position);

            internal uint PositionUInt => (uint)Position;

            internal Node Node;
            internal int Position;
        }

        /// <summary>
        /// Enumerates through the elements of the B-tree in sorted order.
        /// </summary>
        public struct KeyValueEnumerator : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            internal KeyValueEnumerator(Iterator iterator)
            {
                _iterator = iterator;
                --_iterator.Position;
            }

            public bool MoveNext()
            {
                _iterator.Increment();
                return _iterator.Position != _iterator.Node.Count;
            }

            public void Reset() { throw new NotImplementedException(); }
            public void Dispose() { }
            public KeyValuePair<TKey, TValue> Current => _iterator.KeyValue;
            object IEnumerator.Current => Current;

            private Iterator _iterator;
        }

        /// <summary>
        /// Produces an enumerator that enumerates the elements of the B-tree in sorted order.
        /// </summary>
        public struct KeyValueEnumerable : IEnumerable<KeyValuePair<TKey, TValue>>
        {
            internal KeyValueEnumerable(Iterator iterator)
            {
                _iterator = iterator;
            }

            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => new KeyValueEnumerator(_iterator);
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            private Iterator _iterator;
        }

        /// <summary>
        /// Enumerates a range withtin the B-tree in sorted order, from start (inclusive) to end (exclusive).
        /// </summary>
        public struct KeyValueRangeEnumerator : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            internal KeyValueRangeEnumerator(Iterator start, Iterator end)
            {
                _iterator = start;
                _end = end;
                --_iterator.Position;
            }

            public bool MoveNext()
            {
                _iterator.Increment();
                return _iterator.Node != _end.Node || _iterator.Position != _end.Position;
            }

            public void Reset() { throw new NotImplementedException(); }
            public void Dispose() { }
            public KeyValuePair<TKey, TValue> Current => _iterator.KeyValue;
            object IEnumerator.Current => Current;

            private Iterator _iterator;
            private Iterator _end;
        }

        /// <summary>
        /// Produces an enumerator that enumerates a range within the B-tree in sorted order.
        /// </summary>
        public struct KeyValueRangeEnumerable : IEnumerable<KeyValuePair<TKey, TValue>>
        {
            internal KeyValueRangeEnumerable(Iterator start, Iterator end)
            {
                _start = start;
                _end = end;
            }

            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => new KeyValueRangeEnumerator(_start, _end);
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            private Iterator _start;
            private Iterator _end;
        }

        /// <summary>
        /// Used to calculate node stats.
        /// </summary>
        private struct NodeStats
        {
            public NodeStats(ulong leafNodes, ulong internalNodes)
            {
                LeafNodes = leafNodes;
                InternalNodes = internalNodes;
            }

            public readonly ulong LeafNodes;
            public readonly ulong InternalNodes;
        }

        /// <summary>
        /// The main node struct.  This is a wrapper that holds a pointer to the actual node
        /// on the heap, since pointer arithmetic in C# is not possible on <c>this</c>.
        /// 
        /// Nodes can either be internal, leaf, or root.
        /// 
        /// Leaf nodes have only the header + key values, internal nodes are leaf nodes + children,
        /// and the root node is either a leaf node or an internal node + size and rightmost fields.
        /// 
        /// The node structure is:
        /// [NodeHeader]
        /// [Count KeyValuePairs]
        /// [Count + 1 Children Pointers]
        /// [Possible root fields]
        /// </summary>
        internal struct Node
        {
            public static Node Empty => new Node();

            public static bool operator ==(Node a, Node b) => a._node == b._node;
            public static bool operator !=(Node a, Node b) => a._node != b._node;
            public override bool Equals(object obj)
            {
                if (obj is Node)
                    return ((Node)obj) == this;
                else
                    return false;
            }
            public override int GetHashCode() => ((ulong)_node).GetHashCode();


            public Node(NodeHeader* node)
            {
                _node = node;
            }

            public NodeHeader* Ptr => _node;
            public bool NonEmpty => _node != null;
            public bool IsEmpty => _node == null;
            public bool IsLeaf => _node->IsLeaf;
            public int Position
            {
                get { return _node->Position; }

            }
            void SetPosition(int value)
            {
                _node->Position = (CounterType)value;
            }

            public int Count
            {
                get { return _node->Count; }
                set { _node->Count = (CounterType)value; }
            }

            public uint CountUInt => _node->Count;

            public int MaxCount => _node->MaxCount;

            public Node Parent => new Node(_node->Parent);

            public bool IsRoot => _node->Parent->IsLeaf;

            public void MakeRoot()
            {
                if (!Parent.IsRoot)
                    throw new InvalidOperationException("Parent must be root");

                _node->Parent = _node->Parent->Parent;
            }

            internal Node Rightmost
            {
                get { return new Node(Footer->Rightmost); }
                set { Footer->Rightmost = value._node; }
            }

            internal SizeType Size
            {
                get { return Footer->Size; }
                set { Footer->Size = value; }
            }

            public TKey Key(int i)
            {
                return MemUnsafe.Read<TKey>(Keys + i * SIZEOF_KEY_VALUE);
            }

            public TValue Value(int i)
            {
                return MemUnsafe.Read<TValue>(Keys + i * SIZEOF_KEY_VALUE + SIZEOF_KEY);
            }

            public void SetValue(int i, TValue value)
            {
                MemUnsafe.Write<TValue>(Keys + i * SIZEOF_KEY_VALUE + SIZEOF_KEY, ref value);
            }

            public byte* Keys => (((byte*)_node) + SIZEOF_BASE_HEADER);

            public KeyValuePair<TKey, TValue> GetKeyValue(int i)
            {
                return MemUnsafe.Read<KeyValuePair<TKey, TValue>>(Keys + i * SIZEOF_KEY_VALUE);
            }

            public void SetKeyValue(int i, ref KeyValuePair<TKey, TValue> keyValue)
            {
                MemUnsafe.Write<KeyValuePair<TKey, TValue>>(Keys + i * SIZEOF_KEY_VALUE, ref keyValue);
            }

            public void KeyValueSwap(int i, Node other, int j)
            {
                var keys = Keys;
                var otherKeys = other.Keys;
                var ikv = MemUnsafe.Read<KeyValuePair<TKey, TValue>>(keys + i * SIZEOF_KEY_VALUE);
                MemUnsafe.Write(keys + i * SIZEOF_KEY_VALUE, MemUnsafe.Read<KeyValuePair<TKey, TValue>>(otherKeys + j * SIZEOF_KEY_VALUE));
                MemUnsafe.Write(otherKeys + j * SIZEOF_KEY_VALUE, ikv);
            }

            public void KeyValueCopy(int i, Node target, int j)
            {
                MemUnsafe.Write(target.Keys + j * SIZEOF_KEY_VALUE, MemUnsafe.Read<KeyValuePair<TKey, TValue>>(Keys + i * SIZEOF_KEY_VALUE));
            }

            public void KeyValueCopy(int srcIndex, int targetIdx)
            {
                var keys = Keys;
                MemUnsafe.Write(keys + targetIdx * SIZEOF_KEY_VALUE, MemUnsafe.Read<KeyValuePair<TKey, TValue>>(keys + srcIndex * SIZEOF_KEY_VALUE));
            }

            public Node Child(int i)
            {
                Debug.Assert(!IsLeaf);
                return new Node(Children[i]);
            }

            public Node Child(uint i)
            {
                Debug.Assert(!IsLeaf);
                return new Node(Children[i]);
            }

            public Node Child(ulong i)
            {
                Debug.Assert(!IsLeaf);
                return new Node(Children[i]);
            }

            public void SetChild(int i, Node node)
            {
                SetChild(i, node._node);
            }

            public void SetChild(int i, NodeHeader* node)
            {
                Debug.Assert(!IsLeaf);
                Debug.Assert(i < NODE_CHILDREN_COUNT);
                var child = Children + i;
                *child = node;
                node->Parent = _node;
                node->Position = (CounterType)i;
            }

            public NodeHeader** Children
            {
                get
                {
                    Debug.Assert(!IsLeaf);
                    return (NodeHeader**)(((byte*)_node) + CHILDREN_OFFSET);
                }
            }

            private RootFooter* Footer
            {
                get
                {
                    Debug.Assert(IsRoot);
                    return (RootFooter*)(((byte*)_node) + FOOTER_OFFSET);
                }
            }

            internal int LinearSearch(TKey k)
            {
                int s = 0;
                int e = Count;
                var keys = Keys;
                while (s < e)
                {
                    if (!KeyLessThan(MemUnsafe.Read<TKey>(keys + s * SIZEOF_KEY_VALUE), k))
                        return s;
                    ++s;
                }
                return s;
            }

            internal void InsertKeyValue(int i, KeyValuePair<TKey, TValue> x)
            {
                Debug.Assert(i <= Count);
                for (int j = Count; j > i; --j)
                    KeyValueCopy(j - 1, j);
                SetKeyValue(i, ref x);
                Count += 1;

                if (!IsLeaf)
                {
                    var children = Children;
                    ++i;
                    for (int j = Count; j > i; --j)
                    {
                        children[j] = children[j - 1];
                        children[j]->Position = (CounterType)j;
                    }
                    children[i] = null;
                }
            }

            internal void RemoveValue(int i)
            {
                if (!IsLeaf)
                {
                    var children = Children;
                    Debug.Assert(children[i + 1]->Count == 0);
                    for (int j = i + 1; j < Count; ++j)
                    {
                        children[j] = children[j + 1];
                        children[j]->Position = (CounterType)j;
                    }

                }

                Count -= 1;
                for (; i < Count; ++i)
                    KeyValueCopy(i + 1, i);
            }

            public void RebalanceRightToLeft(Node src, int toMove)
            {
                Debug.Assert(Parent == src.Parent);
                Debug.Assert(Position + 1 == src.Position);
                Debug.Assert(src.Count >= Count);
                Debug.Assert(toMove >= 1);
                Debug.Assert(toMove <= src.Count);

                // Move the delimiting value to the left node and the new delimiting value
                // from the right node.
                Parent.KeyValueCopy(Position, this, Count);
                src.KeyValueCopy(toMove - 1, Parent, Position);

                // Move the values from the right to the left node.
                for (int i = 1; i < toMove; ++i)
                {
                    src.KeyValueCopy(i - 1, this, Count + i);
                }
                // Shift the values in the right node to their correct position.
                for (int i = toMove; i < src.Count; ++i)
                {
                    src.KeyValueCopy(i, i - toMove);
                }

                if (!IsLeaf)
                {
                    var srcChildren = src.Children;
                    // Move the child pointers from the right to the left node.
                    for (int i = 0; i < toMove; ++i)
                    {
                        SetChild(1 + Count + i, srcChildren[i]);
                    }
                    for (int i = 0; i <= src.Count - toMove; ++i)
                    {
                        Debug.Assert(i + toMove <= src.MaxCount);
                        srcChildren[i] = srcChildren[i + toMove];
                        srcChildren[i]->Position = (CounterType)i;
                        srcChildren[i + toMove] = null;
                    }
                }

                // Fixup the counts on the src and dest nodes.
                Count += toMove;
                src.Count -= toMove;
            }

            public void RebalanceLeftToRight(Node dest, int toMove)
            {
                Debug.Assert(Parent == dest.Parent);
                Debug.Assert(Position + 1 == dest.Position);
                Debug.Assert(Count >= dest.Count);
                Debug.Assert(toMove >= 1);
                Debug.Assert(toMove <= Count);

                // Make room in the right node for the new values.
                for (int i = dest.Count - 1; i >= 0; --i)
                {
                    dest.KeyValueCopy(i, i + toMove);
                }

                // Move the delimiting value to the right node and the new delimiting value
                // from the left node.
                Parent.KeyValueCopy(Position, dest, toMove - 1);
                KeyValueCopy(Count - toMove, Parent, Position);

                // Move the values from the left to the right node.
                for (int i = 1; i < toMove; ++i)
                {
                    KeyValueCopy(Count - toMove + i, dest, i - 1);
                }

                if (!IsLeaf)
                {
                    var children = Children;
                    var destChildren = dest.Children;
                    // Move the child pointers from the left to the right node.
                    for (int i = dest.Count; i >= 0; --i)
                    {
                        dest.SetChild(i + toMove, destChildren[i]);
                        destChildren[i] = null;
                    }
                    for (int i = 1; i <= toMove; ++i)
                    {
                        dest.SetChild(i - 1, children[Count - toMove + i]);
                        children[Count - toMove + i] = null;
                    }
                }

                // Fixup the counts on the src and dest nodes.
                Count -= toMove;
                dest.Count += toMove;
            }

            public void Split(Node dest, int insertPosition)
            {
                Debug.Assert(dest.Count == 0);

                // We bias the split based on the position being inserted. If we're
                // inserting at the beginning of the left node then bias the split to put
                // more values on the right node. If we're inserting at the end of the
                // right node then bias the split to put more values on the left node.
                if (insertPosition == 0)
                {
                    dest.Count = Count - 1;
                }
                else if (insertPosition == MaxCount)
                {
                    dest.Count = 0;
                }
                else
                {
                    dest.Count = Count / 2;
                }
                Count -= dest.Count;
                Debug.Assert(Count >= 1);

                // Move values from the left sibling to the right sibling.
                for (int i = 0; i < dest.Count; ++i)
                {
                    KeyValueCopy(Count + i, dest, i);
                }

                // The split key is the largest value in the left sibling.
                Count -= 1;
                Parent.InsertKeyValue(Position, default(KeyValuePair<TKey, TValue>));
                KeyValueCopy(Count, Parent, Position);
                Parent.SetChild(Position + 1, dest);

                if (!IsLeaf)
                {
                    var children = Children;
                    for (int i = 0; i <= dest.Count; ++i)
                    {
                        Debug.Assert(children[Count + i + 1] != null);
                        dest.SetChild(i, children[Count + i + 1]);
                        children[Count + i + 1] = null;
                    }
                }
            }

            public void Merge(Node src)
            {
                Debug.Assert(Parent == src.Parent);
                Debug.Assert(Position + 1 == src.Position);

                // Move the delimiting value to the left node.
                Parent.KeyValueCopy(Position, this, Count);

                // Move the values from the right to the left node.
                for (int i = 0; i < src.Count; ++i)
                    src.KeyValueCopy(i, this, 1 + Count + i);

                if (!IsLeaf)
                {
                    var srcChildren = src.Children;
                    // Move the child pointers from the right to the left node.
                    for (int i = 0; i <= src.Count; ++i)
                    {
                        SetChild(1 + Count + i, srcChildren[i]);
                        srcChildren[i] = null;
                    }
                }

                // Fixup the counts on the src and dest nodes.
                Count += 1 + src.Count;
                src.Count = 0;

                // Remove the value on the parent node.
                Parent.RemoveValue(Position);
            }

            public void Swap(Node x)
            {
                Debug.Assert(IsLeaf == x.IsLeaf);

                // Swap the values.
                int n = Math.Max(Count, x.Count);
                for (int i = 0; i < n; ++i)
                {
                    KeyValueSwap(i, x, i);
                }

                if (!IsLeaf)
                {
                    var children = Children;
                    var xChildren = x.Children;
                    // Swap the child pointers.
                    for (int i = 0; i <= n; ++i)
                    {
                        Swap(ref children[i], ref xChildren[i]);
                    }
                    for (int i = 0; i <= Count; ++i)
                    {
                        xChildren[i]->Parent = x._node;
                    }
                    for (int i = 0; i <= x.Count; ++i)
                    {
                        children[i]->Parent = _node;
                    }
                }

                // Swap the counts.
                Swap(ref _node->Count, ref x._node->Count);
            }

            public static void InitLeaf(NodeHeader* node, NodeHeader* parent, int maxCount)
            {
                node->IsLeaf = true;
                node->Position = 0;
                node->MaxCount = (CounterType)maxCount;
                node->Count = 0;
                node->Parent = parent;
                var n = new Node(node);
#if DEBUG
                var val = default(KeyValuePair<TKey, TValue>);
                for (int i = 0; i < maxCount; ++i)
                    n.SetKeyValue(i, ref val);
#endif
            }

            public static void InitInternal(NodeHeader* node, NodeHeader* parent)
            {
                InitLeaf(node, parent, NODE_KV_COUNT);
                node->IsLeaf = false;
                var n = new Node(node);
#if DEBUG
                var children = n.Children;
                for (int i = 0; i < NODE_CHILDREN_COUNT; ++i)
                    children[i] = null;
#endif
            }

            public static void InitRoot(NodeHeader* node, NodeHeader* parent)
            {
                InitInternal(node, parent);
                var n = new Node(node);
                n.Rightmost = new Node(parent);
                n.Size = parent->Count;
            }

            private void Swap(ref CounterType a, ref CounterType b)
            {
                var temp = a;
                a = b;
                b = temp;
            }

            private void Swap(ref NodeHeader* a, ref NodeHeader* b)
            {
                var temp = a;
                a = b;
                b = temp;
            }

            NodeHeader* _node;
        }

        /// <summary>
        /// Create a new B-tree.
        /// </summary>
        /// <param name="allocator">The allocator to use for B-tree nodes.  If null, <see cref="DefaultAllocator"/> will be used.</param>
        public BTree(IAllocator<IntPtr> allocator = null)
        {
            _allocator = allocator ?? new DefaultAllocator();
        }

        /// <summary>
        /// Use a finalizer in case Dispose() is not called.
        /// </summary>
        ~BTree()
        {
            if (_allocator != null)
                Dispose();
        }

        public bool IsEmpty => _root == null;

        /// <summary>
        /// An iterator pointing to the first sorted element.
        /// </summary>
        public Iterator Begin => new Iterator(Leftmost, 0);

        /// <summary>
        /// An iterator pointing to one past the last sorted element.
        /// </summary>
        public Iterator End => new Iterator(Rightmost, Rightmost.NonEmpty ? Rightmost.Count : 0);

        /// <summary>
        /// Get an iterator pointing to the lower bound (exclusive) for this key.
        /// </summary>
        /// <param name="key">The key to search for.</param>
        /// <returns></returns>
        public Iterator LowerBound(TKey key) => InternalLowerBound(key, new Iterator(Root, 0));

        /// <summary>
        /// Get an iterator pointing to the upper bound (exclusive) for this key.
        /// </summary>
        /// <param name="key">The key to search for.</param>
        /// <returns></returns>
        public Iterator UpperBound(TKey key) => InternalUpperBound(key, new Iterator(Root, 0));

        /// <summary>
        /// The total number of bytes used by the B-tree on the heap.
        /// </summary>
        public ulong BytesUsed
        {
            get
            {
                var stats = InternalStats(Root);
                if (stats.LeafNodes == 1 && stats.InternalNodes == 0)
                {
                    return (ulong)(SIZEOF_LEAF_NODE + Root.MaxCount * SIZEOF_KEY_VALUE);
                }
                else
                {
                    return
                        (ulong)SIZEOF_ROOT_NODE - (ulong)SIZEOF_INTERNAL_NODE +
                        stats.LeafNodes * (ulong)SIZEOF_LEAF_NODE +
                        stats.InternalNodes * (ulong)SIZEOF_INTERNAL_NODE;
                }
            }
        }

        /// <summary>
        /// The B-tree per-element overhead in bytes.
        /// </summary>
        public double Overhead => IsEmpty ? 0 : ((double)(BytesUsed - ((ulong)Size * (ulong)(SIZEOF_KEY + SIZEOF_VALUE))) / (double)Size);

        /// <summary>
        /// The number of nodes in the B-tree.
        /// </summary>
        public ulong NodeCount
        {
            get
            {
                var stats = InternalStats(Root);
                return stats.LeafNodes + stats.InternalNodes;
            }
        }

        /// <summary>
        /// How full the nodes of the B-tree are.
        /// </summary>
        public double Fullness => ((double)Size) / (double)(NodeCount * (ulong)NODE_KV_COUNT);

        // ************************************************************************************************
        // The below methods are ported from the C++ version, with performance tweaks for C#, because
        // the CLR is not-so-good at inlining effectively, and sometimes it needs some help with ordering
        // and optimal register allocation.
        //
        // These are kept as a private interface that's used by the public .NET-friendly IDictionary methods.
        // ************************************************************************************************

        private NodeStats InternalStats(Node node)
        {
            if (node.IsEmpty)
                return new NodeStats(0, 0);

            if (node.IsLeaf)
                return new NodeStats(1, 0);

            var res = new NodeStats(0, 1);
            for (int i = 0; i <= node.Count; ++i)
            {
                var stats = InternalStats(node.Child(i));
                res = new NodeStats(res.LeafNodes + stats.LeafNodes, res.InternalNodes + stats.InternalNodes);
            }

            return res;
        }

        private Node NewLeafNode(Node parent)
        {
            var h = (NodeHeader*)_allocator.Allocate(SIZEOF_LEAF_NODE);
            Node.InitLeaf(h, parent.Ptr, NODE_KV_COUNT);
            return new Node(h);
        }

        private Node NewInternalNode(Node parent)
        {
            var h = (NodeHeader*)_allocator.Allocate(SIZEOF_INTERNAL_NODE);
            Node.InitInternal(h, parent.Ptr);
            return new Node(h);
        }

        private Node NewInternalRootNode()
        {
            var h = (NodeHeader*)_allocator.Allocate(SIZEOF_ROOT_NODE);
            Node.InitRoot(h, Root.Parent.Ptr);
            return new Node(h);
        }

        private Node NewLeafRootNode(int maxCount)
        {
            var h = (NodeHeader*)_allocator.Allocate(SIZEOF_BASE_HEADER + SIZEOF_KEY_VALUE * maxCount);
            Node.InitLeaf(h, h, maxCount);
            return new Node(h);
        }

        private void DeleteInternalNode(Node node)
        {
            Debug.Assert(node != Root);
            _allocator.Deallocate((IntPtr)node.Ptr, SIZEOF_INTERNAL_NODE);
        }

        private void DeleteInternalRootNode()
        {
            _allocator.Deallocate((IntPtr)_root, SIZEOF_ROOT_NODE);
        }

        private void DeleteLeafNode(Node node)
        {
            _allocator.Deallocate((IntPtr)node.Ptr, SIZEOF_BASE_HEADER + SIZEOF_KEY_VALUE * node.MaxCount);
        }

        private bool EraseUnique(TKey key)
        {
            var iterator = InternalFindUnique(key, new Iterator(Root, 0));
            if (iterator.Node.IsEmpty)
            {
                // The key doesn't exist in the tree, return nothing done.
                return false;
            }
            Erase(iterator);
            return true;
        }

        private Iterator Erase(Iterator iterator)
        {
            bool internalDelete = false;
            if (!iterator.Node.IsLeaf)
            {
                // Deletion of a value on an internal node. Swap the key with the largest
                // value of our left child. This is easy, we just decrement iterator.
                Iterator tmpIter = iterator;
                iterator.Decrement();
                Debug.Assert(iterator.Node.IsLeaf);
                Debug.Assert(!KeyEquals(tmpIter.Key, iterator.Key));
                iterator.Node.KeyValueSwap(iterator.Position, tmpIter.Node, tmpIter.Position);
                internalDelete = true;
                --Size;
            }
            else if (!Root.IsLeaf)
            {
                --Size;
            }

            // Delete the key from the leaf.
            iterator.Node.RemoveValue(iterator.Position);

            // We want to return the next value after the one we just erased. If we
            // erased from an internal node (internal_delete == true), then the next
            // value is ++(++iterator). If we erased from a leaf node (internal_delete ==
            // false) then the next value is ++iterator. Note that ++iterator may point to an
            // internal node and the value in the internal node may move to a leaf node
            // (iterator.node) when rebalancing is performed at the leaf level.

            // Merge/rebalance as we walk back up the tree.
            var res = iterator;
            for (;;)
            {
                if (iterator.Node == Root)
                {
                    TryShrink();
                    if (IsEmpty)
                    {
                        return new Iterator();
                    }
                    break;
                }
                if (iterator.Node.Count >= MIN_NODE_KV_COUNT)
                {
                    break;
                }
                bool merged = TryMergeOrRebalance(ref iterator);
                if (iterator.Node.IsLeaf)
                {
                    res = iterator;
                }
                if (!merged)
                {
                    break;
                }
                iterator.Node = iterator.Node.Parent;
            }

            // Adjust our return value. If we're pointing at the end of a node, advance
            // the iterator.
            if (res.Position == res.Node.Count)
            {
                res.Position = res.Node.Count - 1;
                res.Increment();
            }
            // If we erased from an internal node, advance the iterator.
            if (internalDelete)
            {
                res.Increment();
            }
            return res;
        }

        private Pair<Iterator, bool> InsertUnique(TKey key, TValue value)
        {
            if (IsEmpty)
                _root = NewLeafRootNode(1).Ptr;

            var node = Root;
            var keys = node.Keys;

        START:
            uint e = node.CountUInt;
            uint s = 0;
            for (;;)
            {
                if (s < e && KeyLessThan(MemUnsafe.Read<TKey>(keys), key))
                {
                    ++s;
                    keys += USIZEOF_KEY_VALUE;
                }
                else
                {
                    if (node.IsLeaf)
                        break;

                    node = node.Child(s);
                    keys = node.Keys;
                    goto START;
                }
            }

            var iterator = new Iterator(node, (int)s);
            var last = InternalLast(iterator);
            if (last.Node.NonEmpty && KeyEquals(key, last.Key))
            {
                // The key already exists in the tree, do nothing.
                return new Pair<Iterator, bool>(last, false);
            }

            return new Pair<Iterator, bool>(InternalInsert(iterator, new KeyValuePair<TKey, TValue>(key, value)), true);
        }


        // NON-INLINED VERSION OF ABOVE
        /*public Pair<Iterator, bool> InsertUnique(TKey key, TValue value)
        {
            if (IsEmpty)
                _root = NewLeafRootNode(1).Ptr;

            var iterator = InternalLocate(key, new Iterator(Root, 0));
            var last = InternalLast(iterator);
            if (last.Node.NonEmpty && EqualTo(key, last.Key))
            {
                // The key already exists in the tree, do nothing.
                return new Pair<Iterator, bool>(last, false);
            }

            return new Pair<Iterator, bool>(InternalInsert(iterator, new KeyValueType(key, value)), true);
        }*/

        private Iterator InternalInsert(Iterator iterator, KeyValuePair<TKey, TValue> value)
        {
            if (!iterator.Node.IsLeaf)
            {
                // We can't insert on an internal Node. Instead, we'll insert after the
                // previous value which is guaranteed to be on a leaf Node.
                iterator.Decrement();
                ++iterator.Position;
            }
            if (iterator.Node.Count == iterator.Node.MaxCount)
            {
                // Make room in the leaf for the new item.
                if (iterator.Node.MaxCount < NODE_KV_COUNT)
                {
                    // Insertion into the root where the root is smaller that the full Node
                    // size. Simply grow the size of the root Node.
                    Debug.Assert(iterator.Node == Root);
                    iterator.Node = NewLeafRootNode(
                        Math.Min(NODE_KV_COUNT, 2 * iterator.Node.MaxCount));
                    iterator.Node.Swap(Root);
                    DeleteLeafNode(Root);
                    _root = iterator.Node.Ptr;
                }
                else
                {
                    RebalanceOrSplit(ref iterator);
                    ++Size;
                }
            }
            else if (!Root.IsLeaf)
            {
                ++Size;
            }
            iterator.Node.InsertKeyValue(iterator.Position, value);
            return iterator;
        }

        private void RebalanceOrSplit(ref Iterator iterator)
        {
            Debug.Assert(iterator.Node.Count == iterator.Node.MaxCount);

            // First try to make room on the node by rebalancing.
            Node parent = iterator.Node.Parent;
            if (iterator.Node != Root)
            {
                if (iterator.Node.Position > 0)
                {
                    // Try rebalancing with our left sibling.
                    Node left = parent.Child(iterator.Node.Position - 1);
                    if (left.Count < left.MaxCount)
                    {
                        // We bias rebalancing based on the position being inserted. If we're
                        // inserting at the end of the right node then we bias rebalancing to
                        // fill up the left node.
                        int toMove = (left.MaxCount - left.Count) / (1 + ((iterator.Position < left.MaxCount) ? 1 : 0));
                        toMove = Math.Max(1, toMove);

                        if (((iterator.Position - toMove) >= 0) ||
                            ((left.Count + toMove) < left.MaxCount))
                        {
                            left.RebalanceRightToLeft(iterator.Node, toMove);

                            Debug.Assert(iterator.Node.MaxCount - iterator.Node.Count == toMove);
                            iterator.Position -= toMove;
                            if (iterator.Position < 0)
                            {
                                iterator.Position += left.Count + 1;
                                iterator.Node = left;
                            }

                            Debug.Assert(iterator.Node.Count < iterator.Node.MaxCount);
                            return;
                        }
                    }
                }

                if (iterator.Node.Position < parent.Count)
                {
                    // Try rebalancing with our right sibling.
                    Node right = parent.Child(iterator.Node.Position + 1);
                    if (right.Count < right.MaxCount)
                    {
                        // We bias rebalancing based on the position being inserted. If we're
                        // inserting at the beginning of the left node then we bias rebalancing
                        // to fill up the right node.
                        int toMove = (right.MaxCount - right.Count) /
                            (1 + ((iterator.Position > 0) ? 1 : 0));
                        toMove = Math.Max(1, toMove);

                        if ((iterator.Position <= (iterator.Node.Count - toMove)) ||
                            ((right.Count + toMove) < right.MaxCount))
                        {
                            iterator.Node.RebalanceLeftToRight(right, toMove);

                            if (iterator.Position > iterator.Node.Count)
                            {
                                iterator.Position = iterator.Position - iterator.Node.Count - 1;
                                iterator.Node = right;
                            }

                            Debug.Assert(iterator.Node.Count < iterator.Node.MaxCount);
                            return;
                        }
                    }
                }

                // Rebalancing failed, make sure there is room on the parent node for a new
                // value.
                if (parent.Count == parent.MaxCount)
                {
                    var parentIter = new Iterator(iterator.Node.Parent, iterator.Node.Position);
                    RebalanceOrSplit(ref parentIter);
                }
            }
            else
            {
                // Rebalancing not possible because this is the root node.
                if (Root.IsLeaf)
                {
                    // The root node is currently a leaf node: create a new root node and set
                    // the current root node as the child of the new root.
                    parent = NewInternalRootNode();
                    parent.SetChild(0, Root);
                    _root = parent.Ptr;
                    Debug.Assert(Rightmost == parent.Child(0));
                }
                else
                {
                    // The root node is an internal node. We do not want to create a new root
                    // node because the root node is special and holds the size of the tree
                    // and a pointer to the rightmost node. So we create a new internal node
                    // and move all of the items on the current root into the new node.
                    parent = NewInternalNode(parent);
                    parent.SetChild(0, parent);
                    parent.Swap(Root);
                    iterator.Node = parent;
                }
            }

            // Split the node.
            Node splitNode;
            if (iterator.Node.IsLeaf)
            {
                splitNode = NewLeafNode(parent);
                iterator.Node.Split(splitNode, iterator.Position);
                if (Rightmost == iterator.Node)
                {
                    Rightmost = splitNode;
                }
            }
            else
            {
                splitNode = NewInternalNode(parent);
                iterator.Node.Split(splitNode, iterator.Position);
            }

            if (iterator.Position > iterator.Node.Count)
            {
                iterator.Position = iterator.Position - iterator.Node.Count - 1;
                iterator.Node = splitNode;
            }
        }

        private bool TryMergeOrRebalance(ref Iterator iterator)
        {
            Node parent = iterator.Node.Parent;
            if (iterator.Node.Position > 0)
            {
                // Try merging with our left sibling.
                Node left = parent.Child(iterator.Node.Position - 1);
                if ((1 + left.Count + iterator.Node.Count) <= left.MaxCount)
                {
                    iterator.Position += 1 + left.Count;
                    MergeNodes(left, iterator.Node);
                    iterator.Node = left;
                    return true;
                }
            }
            if (iterator.Node.Position < parent.Count)
            {
                // Try merging with our right sibling.
                Node right = parent.Child(iterator.Node.Position + 1);
                if ((1 + iterator.Node.Count + right.Count) <= right.MaxCount)
                {
                    MergeNodes(iterator.Node, right);
                    return true;
                }
                // Try rebalancing with our right sibling. We don't perform rebalancing if
                // we deleted the first element from iterator.Node and the node is not
                // empty. This is a small optimization for the common pattern of deleting
                // from the front of the tree.
                if ((right.Count > MIN_NODE_KV_COUNT) &&
                    ((iterator.Node.Count == 0) ||
                     (iterator.Position > 0)))
                {
                    int toMove = (right.Count - iterator.Node.Count) / 2;
                    toMove = Math.Min(toMove, right.Count - 1);
                    iterator.Node.RebalanceRightToLeft(right, toMove);
                    return false;
                }
            }
            if (iterator.Node.Position > 0)
            {
                // Try rebalancing with our left sibling. We don't perform rebalancing if
                // we deleted the last element from iterator.Node and the node is not
                // empty. This is a small optimization for the common pattern of deleting
                // from the back of the tree.
                Node left = parent.Child(iterator.Node.Position - 1);
                if ((left.Count > MIN_NODE_KV_COUNT) &&
                    ((iterator.Node.Count == 0) ||
                     (iterator.Position < iterator.Node.Count)))
                {
                    int to_move = (left.Count - iterator.Node.Count) / 2;
                    to_move = Math.Min(to_move, left.Count - 1);
                    left.RebalanceLeftToRight(iterator.Node, to_move);
                    iterator.Position += to_move;
                    return false;
                }
            }
            return false;
        }

        private void TryShrink()
        {
            if (Root.Count > 0)
            {
                return;
            }
            // Deleted the last item on the root node, shrink the height of the tree.
            if (Root.IsLeaf)
            {
                Debug.Assert(Size == 0);
                DeleteLeafNode(Root);
                _root = null;
            }
            else
            {
                Node child = Root.Child(0);
                if (child.IsLeaf)
                {
                    // The child is a leaf node so simply make it the root node in the tree.
                    child.MakeRoot();
                    DeleteInternalRootNode();
                    _root = child.Ptr;
                }
                else
                {
                    // The child is an internal node. We want to keep the existing root node
                    // so we move all of the values from the child node into the existing
                    // (empty) root node.
                    child.Swap(Root);
                    DeleteInternalNode(child);
                }
            }
        }

        private void MergeNodes(Node left, Node right)
        {
            left.Merge(right);
            if (right.IsLeaf)
            {
                if (Rightmost == right)
                {
                    Rightmost = left;
                }
                DeleteLeafNode(right);
            }
            else
            {
                DeleteInternalNode(right);
            }
        }

        private int Size
        {
            get
            {
                if (IsEmpty)
                    return 0;
                else if (Root.IsLeaf)
                    return Root.Count;
                else
                    return (int)Root.Size;
            }
            set
            {
                var node = new Node(_root);
                node.Size = (SizeType)value;
            }
        }

        private Iterator InternalLowerBound(TKey key, Iterator iterator)
        {
            if (iterator.Node.NonEmpty)
            {
                for (;;)
                {
                    iterator.Position = iterator.Node.LinearSearch(key);
                    if (iterator.Node.IsLeaf)
                    {
                        break;
                    }
                    iterator.Node = iterator.Node.Child(iterator.Position);
                }
                iterator = InternalLast(iterator);
            }

            return iterator;
        }

        private Iterator InternalUpperBound(TKey key, Iterator iterator)
        {
            if (iterator.Node.NonEmpty)
            {
                for (;;)
                {
                    iterator.Position = iterator.Node.LinearSearch(key);
                    if (iterator.Node.IsLeaf)
                    {
                        break;
                    }
                    iterator.Node = iterator.Node.Child(iterator.Position);
                }
            }
            return iterator;
        }

        private Iterator InternalLocate(TKey key, Iterator iterator)
        {
            var node = iterator.Node;
            uint s = iterator.PositionUInt;
            var keys = node.Keys + s * USIZEOF_KEY_VALUE;

        START:
            uint e = node.CountUInt;

            for (;;)
            {
                if (s < e && KeyLessThan(MemUnsafe.Read<TKey>(keys), key))
                {
                    ++s;
                    keys += USIZEOF_KEY_VALUE;
                }
                else
                {
                    if (node.IsLeaf)
                        return new Iterator(node, (int)s);

                    node = node.Child(s);
                    s = 0;
                    keys = node.Keys;
                    goto START;
                }
            }
        }

        private Iterator InternalFindUnique(TKey key, Iterator iterator)
        {
            if (iterator.Node.IsEmpty)
                return new Iterator();

            var node = iterator.Node;
            uint s = iterator.PositionUInt;
            var keys = node.Keys + s * USIZEOF_KEY_VALUE;

        START:
            uint e = node.CountUInt;

            for (;;)
            {
                if (s < e && KeyLessThan(MemUnsafe.Read<TKey>(keys), key))
                {
                    ++s;
                    keys += USIZEOF_KEY_VALUE;
                }
                else
                {
                    if (node.IsLeaf)
                    {
                        iterator = InternalLast(new Iterator(node, (int)s));
                        if (iterator.Node.NonEmpty && KeyEquals(key, iterator.Key))
                            return iterator;
                        else
                            return new Iterator();
                    }

                    node = node.Child(s);
                    s = 0;
                    keys = node.Keys;
                    goto START;
                }
            }
        }

        /* NON-INLINED VERSION OF ABOVE
        private Iterator InternalFindUnique_Original(TKey key, Iterator iterator)
        {
            if (iterator.Node.NonEmpty)
            {
                iterator = InternalLast(InternalLocate(key, iterator));
                if (iterator.Node.NonEmpty && EqualTo(key, iterator.Key))
                    return iterator;
            }
            return new Iterator();
            
        }*/

        private Iterator InternalFindMulti(TKey key, Iterator iterator)
        {
            if (iterator.Node.NonEmpty)
            {
                iterator = InternalLowerBound(key, iterator);
                if (iterator.Node.NonEmpty)
                {
                    iterator = InternalLast(iterator);
                    if (iterator.Node.NonEmpty && KeyEquals(key, iterator.Key))
                    {
                        return iterator;
                    }
                }
            }
            return new Iterator();
        }

        private void InternalClear(Node node)
        {
            if (!node.IsLeaf)
            {
                for (int i = 0; i <= node.Count; ++i)
                {
                    InternalClear(node.Child(i));
                }
                if (node == Root)
                {
                    DeleteInternalRootNode();
                }
                else
                {
                    DeleteInternalNode(node);
                }
            }
            else
            {
                DeleteLeafNode(node);
            }
        }

        private Iterator InternalLast(Iterator iterator)
        {
            while (iterator.Node.NonEmpty && iterator.Position == iterator.Node.Count)
            {
                iterator.Position = iterator.Node.Position;
                iterator.Node = iterator.Node.Parent;
                if (iterator.Node.IsLeaf)
                {
                    iterator.Node = Node.Empty;
                }
            }
            return iterator;
        }

        private Node Leftmost
        {
            get { return _root != null ? new Node(_root->Parent) : Node.Empty; }
        }

        private Node Rightmost
        {
            get { return (_root == null || _root->IsLeaf) ? new Node(_root) : new Node(_root).Rightmost; }
            set
            {
                if (_root == null || _root->IsLeaf)
                    _root = value.Ptr;
                else
                {
                    var root = Root;
                    root.Rightmost = value;
                }
            }
        }

        private Node Root => new Node(_root);

        // ************************************************************************************************
        // End ported private methods
        // ************************************************************************************************

        /// <summary>
        /// Dispose of the B-tree, destroying all nodes and freeing all heap memory.
        /// </summary>
        public void Dispose()
        {
            Clear();
            _allocator = null;
        }

        /// <summary>
        /// Adds the specified key and value to the B-tree. Will throw <see cref="ArgumentException"/> if the key already exists. 
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="value">The value of the element to add. The value can be <see cref="null"/> for reference types.</param>
        public void Add(TKey key, TValue value)
        {
            var res = InsertUnique(key, value);
            if (!res.Second)
                throw new ArgumentException("Cannot add duplicate key", "key");
        }

        /// <summary>
        /// Determines whether the B-tree contains the specified key.
        /// </summary>
        /// <param name="key">The key to locate in the B-tree.</param>
        /// <returns><c>true</c> if the B-tree contains an element with the specified key.</returns>
        public bool ContainsKey(TKey key)
        {
            return Find(key);
        }

        /// <summary>
        /// Removes the element with the specified key from the B-tree.
        /// </summary>
        /// <param name="key">The key of the element to remove.</param>
        /// <returns><c>true</c> if element was removed.</returns>
        public bool Remove(TKey key)
        {
            return EraseUnique(key);
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key of the value to get.</param>
        /// <param name="value">When this method returns, contains the value associated with the specified key, if the key is found; otherwise, the default value for the type of the value parameter. This parameter is passed uninitialized.</param>
        /// <returns><c>true</c> if the B-tree contains an element with the specified key.</returns>
        public bool TryGetValue(TKey key, out TValue value)
        {
            var iter = Find(key);
            if (iter)
            {
                value = iter.Value;
                return true;
            }
            else
            {
                value = default(TValue);
                return false;
            }
        }

        /// <summary>
        /// Adds the specified element to the B-tree. Will throw <see cref="ArgumentException"/> if the key already exists. 
        /// </summary>
        /// <param name="item"></param>
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        /// <summary>
        /// Removes all keys and values from the B-tree.
        /// </summary>
        public void Clear()
        {
            if (_root != null)
            {
                InternalClear(Root);
                _root = null;
            }
        }

        /// <summary>
        /// Determines whether the B-tree contains the specified element.
        /// </summary>
        /// <param name="item">The element to locate in the B-tree.</param>
        /// <returns><c>true</c> if the B-tree contains an element with the specified key and value.</returns>
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            var iter = Find(item.Key);
            if (!iter)
                return false;

            return _valueEqComparer.Equals(iter.Value, item.Value);
        }

        /// <summary>
        /// Copies the elements of the B-tree to an array of type <see cref="KeyValuePair<TKey, TValue>" />, starting at the specified array index.
        /// </summary>
        /// <param name="array">The destination array.</param>
        /// <param name="arrayIndex">The array index to start writing elements.</param>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            for (var iter = Begin; iter; iter.Increment(), ++arrayIndex)
                array[arrayIndex] = iter.KeyValue;
        }

        /// <summary>
        /// Removes the element with the specified key from the B-tree.
        /// </summary>
        /// <param name="item">The element to remove.  Only the key is checked for equality.</param>
        /// <returns><c>true</c> if element was removed.</returns>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return EraseUnique(item.Key);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the B-tree in sorted order.
        /// </summary>
        // <returns>The enumerator.</returns>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new KeyValueEnumerator(Begin);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the B-tree in sorted order.
        /// </summary>
        /// <returns>The enumerator.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return new KeyValueEnumerator(Begin);
        }

        /// <summary>
        /// Returns an enumerable that can iterate through a range in the B-tree in sorted order.
        /// </summary>
        /// <param name="start">The starting key, inclusive.  If the key does not exist, the next highest existing key will be used.</param>
        /// <param name="end">The ending key, exclusive.  The key does not need to exist.</param>
        /// <returns>The range enumerable.</returns>
        public KeyValueRangeEnumerable Range(TKey start, TKey end)
        {
            var lowerIter = LowerBound(start);
            var upperIter = UpperBound(end);
            return new KeyValueRangeEnumerable(lowerIter, upperIter);
        }

        /// <summary>
        /// Returns an enumerable that can iterate from a starting key through the B-tree in sorted order.
        /// </summary>
        /// <param name="start">The starting key, inclusive.  If the key does not exist, the next highest existing key will be used.</param>
        /// <returns>The enumerable.</returns>
        public KeyValueEnumerable From(TKey start)
        {
            var lowerIter = InternalLowerBound(start, Begin);
            return new KeyValueEnumerable(lowerIter);
        }

        /// <summary>
        /// Not implemented yet.
        /// </summary>
        public ICollection<TKey> Keys
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Not implemented yet.
        /// </summary>
        public ICollection<TValue> Values
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// The number of elements in the B-tree.
        /// </summary>
        public int Count => Size;

        /// <summary>
        /// The B-tree is never read-only, always returns false.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Gets or sets the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key of the value to get or set.</param>
        /// <returns>The value associated with the specified key. If the specified key is not found, a get operation throws a <see cref="KeyNotFoundException" />, and a set operation creates a new element with the specified key.</returns>
        public TValue this[TKey key]
        {
            get
            {
                var iter = Find(key);
                if (!iter)
                    throw new KeyNotFoundException();
                else
                    return iter.Value; 
            }
            set
            {
                var res = InsertUnique(key, value);
                if (!res.Second)
                {
                    var iter = res.First;
                    iter.Value = value;
                }
            }
        }

        /// <summary>
        /// Gets an iterator pointing to the specified key.  If no key is found, the iterator is invalid.
        /// </summary>
        /// <param name="key">The key to locate in the B-tree.</param>
        /// <returns>An iterator pointing to the specified key.  If the specified key is not found, the iterator is invalid.</returns>
        public Iterator Find(TKey key)
        {
            return InternalFindUnique(key, new Iterator(Root, 0));
        }

        private NodeHeader* _root;
        private IAllocator<IntPtr> _allocator;
    }

    /// <summary>
    /// A high-performance, space-efficient B-tree implementation using unmanaged memory.
    /// </summary>
    /// <typeparam name="TKey">The key type. MUST NOT CONTAIN ANY MANAGED OBJECTS.</typeparam>
    /// <typeparam name="TValue">Type value type. MUST NOT CONTAIN ANY MANAGED OBJECTS.</typeparam>
    /// <typeparam name="TKeyComparer">The custom key comparer used to sort keys.</typeparam>
    public class BTree<TKey, TValue, TKeyComparer> : BTree<TKey, TValue, TKeyComparer, DefaultBTreeConfig>
        where TKey : struct
        where TValue : struct
        where TKeyComparer : struct, IKeyComparer<TKey>
    {
        /// <summary>
        /// Create a new B-tree.
        /// </summary>
        /// <param name="allocator">The allocator to use for B-tree nodes.  If null, <see cref="DefaultAllocator"/> will be used.</param>
        public BTree(IAllocator<IntPtr> allocator = null)
            : base(allocator)
        {
        }
    }

    /// <summary>
    /// A high-performance, space-efficient B-tree implementation using unmanaged memory.
    /// </summary>
    /// <typeparam name="TKey">The key type.  MUST NOT CONTAIN ANY MANAGED OBJECTS.</typeparam>
    /// <typeparam name="TValue">Type value type.  MUST NOT CONTAIN ANY MANAGED OBJECTS.</typeparam>
    public class BTree<TKey, TValue> : BTree<TKey, TValue, DefaultKeyComparer<TKey>>
        where TKey : struct, IComparable<TKey>
        where TValue : struct
    {
        /// <summary>
        /// Create a new B-tree.
        /// </summary>
        /// <param name="allocator">The allocator to use for B-tree nodes.  If null, <see cref="DefaultAllocator"/> will be used.</param>
        public BTree(IAllocator<IntPtr> allocator = null)
            : base(allocator)
        {
        }
    }
}
