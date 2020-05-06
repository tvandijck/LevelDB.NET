using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace LevelDB.NET
{
    internal class Block : IEnumerable<KeyValuePair<byte[], byte[]>>
    {
        private readonly List<byte[]> m_keys = new List<byte[]>();
        private readonly List<byte[]> m_values = new List<byte[]>();
        private static readonly IComparer<byte[]> s_comparer = new BytewiseComparator();

        public Block(Slice data)
        {
            if (data.Length < sizeof(uint))
            {
                throw new Exception("bad block contents");
            }

            // restart points are recorded at the end of the buffer.
            uint numRestarts = Coding.DecodeFixed32(data.NewSlice(data.Length - sizeof(uint), sizeof(uint)));
            uint restartOffset = data.Length - (1 + numRestarts) * sizeof(uint);

            // we don't really care for these except the first, but we'll parse them.
            var restartPoints = new uint[numRestarts];
            for (uint i = 0; i < numRestarts; ++i)
            {
                var offset = restartOffset + (i * sizeof(uint));
                restartPoints[i] = Coding.DecodeFixed32(data.NewSlice(offset, sizeof(uint)));
            }

            // block data is the remaining data.
            var blockData = data.NewSlice(0, restartOffset);

            // parse this block.
            var key = new InternalKey();
            var value = blockData.NewSlice(0, 0);

            uint m_current = restartPoints[0];
            while (m_current < blockData.Length)
            {
                Slice? p = blockData.NewSlice(m_current);

                // Decode next entry
                p = DecodeEntry(p, out uint shared, out uint non_shared, out uint value_length);
                if (p == null || key.Length < shared)
                {
                    throw new Exception("bad entry in block");
                }
                else
                {
                    key.Resize(shared);
                    key.Append(p.NewSlice(0, non_shared));

                    value.Update(p.Offset + non_shared, value_length);

                    // don't add deleted keys to the dictionary.
                    var parsedKey = new Key(key.Slice(0, key.Length));
                    if (parsedKey.Type == KeyType.kTypeValue)
                    {
                        m_keys.Add(parsedKey.UserKey);
                        m_values.Add(value.ToArray());
                    }
                }

                m_current = value.Offset + value.Length;
            }
        }

        public bool BinarySearch(byte[] key, [NotNullWhen(true)] out byte[]? value)
        {
            int idx = m_keys.BinarySearch(key, s_comparer);
            if (idx >= 0)
            {
                value = m_values[idx];
                return true;
            }

            idx = ~idx;
            if (idx < m_keys.Count)
            {
                value = m_values[idx];
                return true;
            }

            value = null;
            return false;
        }

        public bool TryGetValue(byte[] key, [NotNullWhen(true)] out byte[]? value)
        {
            int idx = m_keys.BinarySearch(key, s_comparer);
            if (idx >= 0)
            {
                value = m_values[idx];
                return true;
            }

            value = null;
            return false;
        }

        public bool ContainsKey(byte[] key)
        {
            return m_keys.BinarySearch(key, s_comparer) >= 0;
        }

        private static Slice? DecodeEntry(Slice p, out uint shared, out uint non_shared, out uint value_length)
        {
            if (p.Length < 3)
            {
                shared = 0;
                non_shared = 0;
                value_length = 0;
                return null;
            }

            shared = p[0];
            non_shared = p[1];
            value_length = p[2];
            if ((shared | non_shared | value_length) < 128)
            {
                // Fast path: all three values are encoded in one byte each
                return p.NewSlice(3);
            }
            else
            {
                shared = Coding.DecodeVarint32(p);
                non_shared = Coding.DecodeVarint32(p);
                value_length = Coding.DecodeVarint32(p);

                if (p.Length < non_shared + value_length)
                {
                    return null;
                }

                return p;
            }
        }

        private IEnumerable<KeyValuePair<byte[], byte[]>> GetAllItems()
        {
            for (int i = 0; i < m_keys.Count; ++i)
            {
                yield return new KeyValuePair<byte[], byte[]>(m_keys[i], m_values[i]);
            }
        }

        public IEnumerator<KeyValuePair<byte[], byte[]>> GetEnumerator()
        {
            return GetAllItems().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetAllItems().GetEnumerator();
        }
    }
}

