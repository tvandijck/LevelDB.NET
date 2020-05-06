using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace LevelDB.NET
{
    public class BytewiseComparator : IComparer<byte[]>
    {
        public static int compare(byte[] a, byte[] b)
        {
            return MemoryExtensions.SequenceCompareTo<byte>(a, b);
        }

        public int Compare([AllowNull] byte[] x, [AllowNull] byte[] y)
        {
            return MemoryExtensions.SequenceCompareTo<byte>(x, y);
        }
    }
}
