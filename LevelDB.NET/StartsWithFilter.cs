using System;

namespace LevelDB.NET
{
    public class StartsWithFilter : IFilter
    {
        private readonly byte[] m_key;

        public StartsWithFilter(byte[] key)
        {
            m_key = key;
        }

        public int Compare(byte[] key)
        {
            int minLen = Math.Min(key.Length, m_key.Length);
            var cut = key.AsSpan().Slice(0, minLen);
            return MemoryExtensions.SequenceCompareTo<byte>(m_key, cut);
        }
    }
}
