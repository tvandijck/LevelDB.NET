using System;
using System.Diagnostics.CodeAnalysis;

namespace LevelDB.NET
{
    internal class Record
    {
        private byte[]? m_bytes = null;
        private int m_length;

        public byte[] Bytes
        {
            get { return m_bytes ?? new byte[0]; }
        }

        public void AddBytes(byte[] bytes)
        {
            AssureCapacity(ref m_bytes, m_length + bytes.Length);
            Array.Copy(bytes, 0, m_bytes, m_length, bytes.Length);
            m_length += bytes.Length;
        }

        public Span<byte> Reserve(int length)
        {
            AssureCapacity(ref m_bytes, m_length + length);
            var result = m_bytes.AsSpan().Slice(m_length, length);
            m_length += length;
            return result;
        }

        private static void AssureCapacity([NotNull] ref byte[]? storage, int length)
        {
            if (storage == null || storage.Length < length)
            {
                var newBytes = new byte[length];
                if (storage != null)
                {
                    Array.Copy(storage, newBytes, storage.Length);
                }
                storage = newBytes;
            }
        }
    }
}
