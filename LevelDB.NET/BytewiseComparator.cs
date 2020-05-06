using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace LevelDB.NET
{
    [Comparator("leveldb.BytewiseComparator")]
    public class BytewiseComparator : IComparer<byte[]>
    {
        public int Compare([AllowNull] byte[] x, [AllowNull] byte[] y)
        {
            return MemoryExtensions.SequenceCompareTo<byte>(x, y);
        }
    }
}
