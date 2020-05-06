using System;
using System.Diagnostics;

namespace LevelDB.NET
{
    internal static class Coding
    {
        public static uint DecodeFixed32(Slice ptr)
        {
            Debug.Assert(ptr.Length >= 4);

            return unchecked((uint)ptr[0]) |
                   (unchecked((uint)ptr[1]) << 8) |
                   (unchecked((uint)ptr[2]) << 16) |
                   (unchecked((uint)ptr[3]) << 24);
        }

        public static ulong DecodeFixed64(Slice ptr)
        {
            Debug.Assert(ptr.Length >= 8);

            return unchecked((ulong)ptr[0]) |
                   (unchecked((ulong)ptr[1]) << 8) |
                   (unchecked((ulong)ptr[2]) << 16) |
                   (unchecked((ulong)ptr[3]) << 24) |
                   (unchecked((ulong)ptr[4]) << 32) |
                   (unchecked((ulong)ptr[5]) << 40) |
                   (unchecked((ulong)ptr[6]) << 48) |
                   (unchecked((ulong)ptr[7]) << 56);
        }

        public static ulong DecodeVarint64(Slice ptr)
        {
            ulong result = 0;
            for (int shift = 0; shift <= 63 && ptr.Length > 0; shift += 7)
            {
                var b = unchecked((ulong)ptr.ReadByte());
                if ((b & 128) != 0)
                {
                    result |= (b & 127) << shift;
                }
                else
                {
                    result |= b << shift;
                    return result;
                }
            }

            throw new Exception("error decoding Varint64");
        }

        public static byte[] DecodeLengthPrefixed(Slice slice)
        {
            var length = DecodeVarint32(slice);
            return slice.ReadBytes(length);
        }

        public static uint DecodeVarint32(Slice ptr)
        {
            uint result = 0;
            for (int shift = 0; shift <= 28 && ptr.Length > 0; shift += 7)
            {
                var b = unchecked((uint)ptr.ReadByte());
                if ((b & 128) != 0)
                {
                    result |= (b & 127) << shift;
                }
                else
                {
                    result |= b << shift;
                    return result;
                }
            }

            throw new Exception("error decoding Varint64");
        }
    }
}
