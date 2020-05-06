using System;

namespace LevelDB.NET
{
    internal class InternalKey
    {
        private byte[] m_bytes = new byte[128];
        private uint m_length = 0;

        public void Clear()
        {
            m_length = 0;
        }

        public uint Length
        {
            get { return m_length; }
        }

        public void Resize(uint size)
        {
            m_length = size;
        }

        public void Append(Slice slice)
        {
            for (uint i = 0; i < slice.Length; ++i)
            {
                m_bytes[m_length] = slice[i];
                m_length++;
            }
        }

        public Slice Slice(uint offset, uint length)
        {
            offset = Math.Min(m_length, offset);
            length = Math.Min(m_length - offset, length);
            return new Slice(m_bytes, offset, length);
        }
    }
}
