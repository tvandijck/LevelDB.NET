using System;

namespace LevelDB.NET
{
    internal class Footer
    {
        public const uint kEncodedLength = 2 * BlockHandle.kMaxEncodedLength + 8;
        public const ulong kTableMagicNumber = 0xdb4775248b80fb57;

        public BlockHandle MetaIndexHandle { get; }
        public BlockHandle IndexHandle { get; }

        public Footer(BlockHandle metaIndexHandle, BlockHandle indexHandle)
        {
            MetaIndexHandle = metaIndexHandle;
            IndexHandle = indexHandle;
        }

        public static Footer DecodeFrom(Slice slice)
        {
            uint startOffset = slice.Offset;
            uint startLength = slice.Length;

            uint magic_lo = Coding.DecodeFixed32(slice.NewSlice(kEncodedLength - 8, 4));
            uint magic_hi = Coding.DecodeFixed32(slice.NewSlice(kEncodedLength - 4, 4));
            ulong magic = (unchecked((ulong)magic_hi) << 32) | unchecked((ulong)magic_lo);

            if (magic != kTableMagicNumber)
            {
                throw new Exception("not an sstable (bad magic number)");
            }

            var metaIndexHandle = BlockHandle.DecodeFrom(slice);
            var indexHandle = BlockHandle.DecodeFrom(slice);

            slice.Update(startOffset + kEncodedLength, startLength - kEncodedLength);
            return new Footer(metaIndexHandle, indexHandle);
        }
    }
}
