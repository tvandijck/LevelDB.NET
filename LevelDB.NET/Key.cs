using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace LevelDB.NET
{
    internal enum KeyType
    {
        kTypeDeletion = 0x0,
        kTypeValue = 0x1
    }

    internal class Key
    {
        public byte[] UserKey { get; }
        public ulong Sequence { get; }
        public KeyType Type { get; }

        public Key(byte[] data)
            : this(new Slice(data))
        {
        }

        public Key(Slice data)
        {
            uint n = data.Length;
            Debug.Assert(n >= 8);

            ulong num = Coding.DecodeFixed64(data.NewSlice(n - 8, 8));

            byte c = unchecked((byte)(num & 0xff));
            Debug.Assert(c <= (byte)KeyType.kTypeValue);

            Type = (KeyType)c;
            Sequence = num >> 8;
            UserKey = data.NewSlice(0, n - 8).ToArray();
        }

        public override string ToString()
        {
            return Encoding.ASCII.GetString(UserKey);
        }
    }
}
