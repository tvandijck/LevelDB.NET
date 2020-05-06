using System;
using System.Collections;
using System.Collections.Generic;

namespace LevelDB.NET
{
    internal class Slice : IEnumerable<byte>
    {
        private readonly byte[] m_bytes;
        private uint m_offset;
        private uint m_length;

        public Slice(byte[] bytes)
        {
            m_bytes = bytes;
            m_length = (uint)bytes.Length;
            m_offset = 0;
        }

        public Slice(byte[] bytes, uint offset, uint length)
        {
            m_bytes = bytes;
            m_offset = Math.Min((uint)m_bytes.Length, offset);
            m_length = Math.Min((uint)m_bytes.Length - m_offset, length);
        }

        internal byte[] ToArray()
        {
            var result = new byte[m_length];
            Array.Copy(m_bytes, m_offset, result, 0, m_length);
            return result;
        }

        public uint Length
        {
            get { return m_length; }
        }

        public uint Offset
        {
            get { return m_offset; }
        }

        public byte this[uint index]
        {
            get { return m_bytes[m_offset + index]; }
        }

        public Slice NewSlice(uint offset, uint length)
        {
            return new Slice(m_bytes, m_offset + offset, length);
        }

        public Slice NewSlice(uint offset)
        {
            return new Slice(m_bytes, m_offset + offset, m_length - offset);
        }

        public void Update(uint offset, uint length)
        {
            m_offset = offset;
            m_length = length;
        }

        public byte ReadByte()
        {
            var result = m_bytes[m_offset];
            m_offset++;
            m_length--;
            return result;
        }

        public byte[] ReadBytes(uint length)
        {
            var result = new byte[length];
            for (int i=0; i<length; ++i)
            {
                result[i] = m_bytes[m_offset+i];
            }

            m_offset+= length;
            m_length-= length;
            return result;
        }

        private class SliceEnumerator : IEnumerator<byte>
        {
            private readonly byte[] m_bytes;
            private readonly uint m_offset;
            private readonly uint m_length;
            private int m_index;

            public SliceEnumerator(byte[] bytes, uint offset, uint length)
            {
                m_bytes = bytes;
                m_length = length;
                m_offset = offset;
                m_index = -1;
            }

            public byte Current
            {
                get { return m_bytes[m_offset + m_index]; }
            }

            object? IEnumerator.Current
            {
                get { return m_bytes[m_offset + m_index]; }
            }

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                m_index++;
                return (m_index >= 0) && (m_index < m_length);
            }

            public void Reset()
            {
                m_index = -1;
            }
        }

        public IEnumerator<byte> GetEnumerator()
        {
            return new SliceEnumerator(m_bytes, m_offset, m_length);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new SliceEnumerator(m_bytes, m_offset, m_length);
        }
    }
}
