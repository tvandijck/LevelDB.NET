using System;

namespace LevelDB.NET
{
    internal struct BlockHandle : IComparable<BlockHandle>
    {
        public const uint kMaxEncodedLength = 10 + 10;

        public ulong Offset { get; }
        public ulong Size { get; }

        public BlockHandle(ulong offset, ulong size)
        {
            Offset = offset;
            Size = size;
        }

        public static BlockHandle DecodeFrom(Slice input)
        {
            var offset = Coding.DecodeVarint64(input);
            var size = Coding.DecodeVarint64(input);
            return new BlockHandle(offset, size);
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(obj, null))
            {
                return false;
            }

            if (obj is BlockHandle)
            {
                BlockHandle other = (BlockHandle)obj;
                return Offset == other.Offset && Size == other.Size;
            }

            return false;
        }

        public override int GetHashCode()
        {
            return Offset.GetHashCode() ^ Size.GetHashCode();
        }

        public override string ToString()
        {
            return $"{Offset}, {Size}";
        }

        public int CompareTo(BlockHandle other)
        {
            int s = Offset.CompareTo(other.Offset);
            if (s == 0)
            {
                s = Size.CompareTo(other.Size);
            }
            return s;
        }
    }
}
