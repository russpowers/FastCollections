using FastCollections.Unsafe;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using FluentAssertions;

namespace FastCollections.Tests.Unsafe
{
    public class BTreeTests : IDisposable
    {
        BTree<int, int> tree = new BTree<int, int>();
        Random rand = new Random();

        public void Dispose()
        {
            tree.Dispose();
        }

        [Fact]
        public void Count_EmptyTree_ShouldBeZero()
        {
            Assert.Equal(tree.Count, 0);
        }

        [Fact]
        public void AddOneItem_ShouldHaveThatItem()
        {
            tree.Add(1, 100);
            tree.Count.Should().Be(1);
            tree[1].Should().Be(100);
        }

        [Fact]
        public void AddOneItemAndRemoveIt_ShouldBeEmptyTree()
        {
            tree.Add(1, 100);
            tree.Remove(1);
            tree.Count.Should().Be(0);
            Action action = () => { var x = tree[1]; };
            action.ShouldThrow<KeyNotFoundException>();
        }

        [Fact]
        public void AddOneItemAndRemoveIt_ShouldThrowOnIndexer()
        {
            tree.Add(1, 100);
            tree.Remove(1);
            Action action = () => { var x = tree[1]; };
            action.ShouldThrow<KeyNotFoundException>();
        }

        [Fact]
        public void Indexer_AddNewItem_ShouldHaveThatItem()
        {
            tree[1] = 100;
            tree.Count.Should().Be(1);
            tree[1].Should().Be(100);
        }

        [Fact]
        public void Indexer_ReplaceValue_ShouldHaveNewValue()
        {
            tree.Add(1, 100);
            tree[1] = 200;
            tree[1].Should().Be(200);
        }

        [Fact]
        public void Add1Item_ShouldBeInEnumerable()
        {
            tree.Add(1, 100);
            tree.Count().Should().Be(1);
            tree.First().Should().Be(new KeyValuePair<int, int>(1, 100));
        }

        [Fact]
        public void Add1Item_ShouldBeInRange()
        {
            tree.Add(1, 100);
            var rangeEnum = tree.Range(1, 1000);
            rangeEnum.Count().Should().Be(1);
            rangeEnum.First().Should().Be(new KeyValuePair<int, int>(1, 100));
        }

        [Fact]
        public void Add1ItemAndGetRangeFrom1To1_ShouldBeEmpty()
        {
            tree.Add(1, 100);
            var rangeEnum = tree.Range(1, 1);
            rangeEnum.Count().Should().Be(0);
        }

        [Fact]
        public void Add1ItemAndGetRangeFrom0To2_ShouldBeInRange()
        {
            tree.Add(1, 100);
            var rangeEnum = tree.Range(0, 2);
            rangeEnum.Count().Should().Be(1);
            rangeEnum.First().Should().Be(new KeyValuePair<int, int>(1, 100));
        }

        [Fact]
        public void Add2Items_StartOfRangeShouldGoToNextIfDoesntExist()
        {
            tree.Add(1, 100);
            tree.Add(5, 101);
            var rangeEnum = tree.Range(3, 20);
            rangeEnum.Count().Should().Be(1);
            rangeEnum.First().Should().Be(new KeyValuePair<int, int>(5, 101));
        }

        [Fact]
        public void Add2Items_EndOfRangeShouldGoToPrevIfDoesntExist()
        {
            tree.Add(1, 100);
            tree.Add(5, 101);
            var rangeEnum = tree.Range(0, 3);
            rangeEnum.Count().Should().Be(1);
            rangeEnum.First().Should().Be(new KeyValuePair<int, int>(1, 100));
        }

        [Fact]
        public void Add100ItemsAndGetRangeForLastHalf_RangeShouldContainLastHalf()
        {
            for (int i = 0; i < 100; ++i)
                tree.Add(i, 100 + i);

            var rangeEnum = tree.Range(50, 100);
            rangeEnum.Count().Should().Be(50);
            var index = 50;
            foreach (var item in rangeEnum)
            {
                item.Key.Should().Be(index);
                ++index;
            }
        }

        [Fact]
        public void Add1000Items_ShouldHaveItemsInSortedOrder()
        {
            var items = new int[1000];
            for (int i = 0; i < 1000; ++i)
                items[i] = i;
            items.Shuffle(10);

            foreach (var item in items)
                tree.Add(item, item + 1000);

            tree.Count.Should().Be(1000);
            var index = 0;
            foreach (var item in tree)
            {
                item.Key.Should().Be(index);
                item.Value.Should().Be(index + 1000);
                ++index;
            }
        }

        [Fact]
        public void Add1000ItemsThenDeleteEvens_ShouldHaveOddsInSortedOrder()
        {
            var items = new int[1000];
            for (int i = 0; i < 1000; ++i)
                items[i] = i;
            items.Shuffle(10);

            foreach (var item in items)
                tree.Add(item, item + 1000);

            foreach (var item in items)
            {
                if (item % 2 == 0)
                    tree.Remove(item);
            }

            tree.Count.Should().Be(500);
            var index = 1;
            foreach (var item in tree)
            {
                item.Key.Should().Be(index);
                item.Value.Should().Be(index + 1000);
                index += 2;
            }
        }
    }
}
