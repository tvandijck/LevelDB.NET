using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace LevelDB.NET
{
    internal class TableFile : IDisposable, IEnumerable<KeyValuePair<byte[], byte[]>>
    {
        public uint Level { get; }
        public ulong FileNr { get; }
        public ulong FileSize { get; }
        public Key Smallest { get; }
        public Key Largest { get; }

        private FileStream? m_stream;
        private Block? m_indexBlock;
        private LruCache<BlockHandle, Block>? m_blockCache;

        private const int kNoCompression = 0x0;
        private const int kSnappyCompression = 0x1;

        public TableFile(uint level, ulong fileNr, ulong fileSize, Key smallest, Key largest)
        {
            Level = level;
            FileNr = fileNr;
            FileSize = fileSize;
            Smallest = smallest;
            Largest = largest;
        }

        public bool ContainsKey(byte[] key)
        {
            if (m_indexBlock != null && m_indexBlock.BinarySearch(key, out var handle))
            {
                var block = GetBlock(handle);
                return block.ContainsKey(key);
            }

            return false;
        }

        public bool TryGetValue(byte[] key, out byte[]? value)
        {
            if (m_indexBlock != null && m_indexBlock.BinarySearch(key, out var handle))
            {
                var block = GetBlock(handle);
                return block.TryGetValue(key, out value);
            }

            value = null;
            return false;
        }

        private Block GetBlock(byte[] key)
        {
            if (m_stream != null)
            {
                var handle = BlockHandle.DecodeFrom(new Slice(key));
                if (m_blockCache.TryGetValue(handle, out var block))
                {
                    return block;
                }

                block = ReadBlock(m_stream, handle);
                m_blockCache.Add(handle, block);
                return block;
            }
            throw new Exception("TableFile not open");
        }

        public void Dispose()
        {
            Close();
        }

        public void AssureOpen(string path, LruCache<BlockHandle, Block> sharedCache)
        {
            if (m_stream == null)
            {
                m_stream = File.OpenRead(Path.Combine(path, $"{FileNr:D6}.ldb"));

                var footerBytes = new byte[Footer.kEncodedLength];
                m_stream.Position = m_stream.Length - Footer.kEncodedLength;
                m_stream.Read(footerBytes);

                var footer = Footer.DecodeFrom(new Slice(footerBytes));

                m_indexBlock = ReadBlock(m_stream, footer.IndexHandle);
                m_blockCache = sharedCache;
            }
        }

        public void Close()
        {
            m_stream?.Dispose();
            m_stream = null;

            m_indexBlock = null;
            m_blockCache = null;
        }

        private static Block ReadBlock(FileStream stream, BlockHandle handle)
        {
            uint n = (uint)handle.Size;

            stream.Position = (long)handle.Offset;
            using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                var buf = reader.ReadBytes(unchecked((int)n + 1));
                var encoding = buf[n];
                var crc32c = reader.ReadInt32();

                // TODO: validate crc32c here?

                switch (encoding)
                {
                    case kNoCompression:
                        return new Block(new Slice(buf, 0, n));

                    case kSnappyCompression:
                        buf = SnappyDecompressor.Decompress(buf, 0, unchecked((int)n));
                        return new Block(new Slice(buf));
                }
            }

            throw new Exception("bad block type");
        }

        private IEnumerable<KeyValuePair<byte[], byte[]>> GetAllItems()
        {
            if (m_indexBlock == null)
            {
                yield break;
            }

            foreach (var indexItem in m_indexBlock)
            {
                var block = GetBlock(indexItem.Value);
                foreach (var item in block)
                {
                    yield return item;
                }
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
