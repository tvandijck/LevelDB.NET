using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;

namespace LevelDB.NET
{
    public partial class Table : IDisposable, IReadOnlyDictionary<byte[], byte[]>
    {
        private readonly VersionSet m_versionSet;
        private readonly string m_path;
        private readonly LruCache<BlockHandle, Block> m_sharedCache = new LruCache<BlockHandle, Block>(16);

        private Table(string path, VersionSet versionSet)
        {
            m_versionSet = versionSet;
            m_path = path;
        }

        public void Dispose()
        {
            m_versionSet.Dispose();
        }

        public IEnumerable<byte[]> Keys
        {
            get { return GetAllItems().Select(s => s.Key); }
        }

        public IEnumerable<byte[]> Values
        {
            get { return GetAllItems().Select(s => s.Value); }
        }

        public int Count
        {
            get { throw new NotSupportedException("Count would have to parse the entire database"); }
        }

        public byte[] this[byte[] key]
        {
            get
            {
                if (TryGetValue(key, out var result))
                {
                    return result;
                }
                throw new Exception("Key not found in Table");
            }
        }

#pragma warning disable CS8614 // Nullability of reference types in type of parameter doesn't match implicitly implemented member.
        public bool TryGetValue(byte[] key, [NotNullWhen(true)] out byte[]? value)
#pragma warning restore CS8614 // Nullability of reference types in type of parameter doesn't match implicitly implemented member.
        {
            if (TryFindFile(key, out var file))
            {
                file.AssureOpen(m_path, m_sharedCache);
                return file.TryGetValue(key, out value);
            }

            value = null;
            return false;
        }

        private bool TryFindFile(byte[] key, [NotNullWhen(true)] out TableFile? file)
        {
            // This could probably be a binary seach, but I'm too lazy.
            foreach (var f in m_versionSet.Files)
            {
                if (BytewiseComparator.compare(key, f.Smallest.UserKey) >= 0 &&
                    BytewiseComparator.compare(key, f.Largest.UserKey) <= 0)
                {
                    file = f;
                    return true;
                }
            }

            file = null;
            return false;
        }

        public static Table OpenRead(string directory)
        {
            // get the filename of the manifest.
            var current = File.ReadAllLines(Path.Combine(directory, "CURRENT"));
            if (current.Length < 1)
            {
                throw new Exception("Invalid CURRENT file.");
            }

            // read the manifest file.
            var versionSet = new VersionSet();

            var block = new byte[32768];
            using (var file = File.OpenRead(Path.Combine(directory, current[0])))
            {
                var recordReader = new RecordReader();
                int numRead = 0;
                do
                {
                    numRead = file.Read(block);
                    if (numRead > 0)
                    {
                        var sequence = new ReadOnlySequence<byte>(block);
                        var reader = new SequenceReader<byte>(sequence);
                        while (reader.Remaining > 6)
                        {
                            if (!recordReader.From(ref reader, record => versionSet.Add(record)))
                            {
                                break;
                            }
                        }
                    }
                } while (numRead >= block.Length);
            }

            return new Table(directory, versionSet);
        }

        public bool ContainsKey(byte[] key)
        {
            if (TryFindFile(key, out var file))
            {
                file.AssureOpen(m_path, m_sharedCache);
                return file.ContainsKey(key);
            }

            return false;
        }

        private IEnumerable<KeyValuePair<byte[], byte[]>> GetAllItems()
        {
            foreach (var f in m_versionSet.Files)
            {
                f.AssureOpen(m_path, m_sharedCache);
                foreach (var item in f)
                {
                    yield return item;
                }
                f.Close();
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

        private class RecordReader
        {
            private Record m_record = new Record();

            public bool From(ref SequenceReader<byte> reader, Action<Record> action)
            {
                reader.TryReadLittleEndian(out int checkSum);
                reader.TryReadLittleEndian(out short length);
                reader.TryRead(out byte type);
                if (type == 0)
                {
                    return false;
                }

                var bytes = m_record.Reserve(length);
                if (reader.TryCopyTo(bytes))
                {
                    reader.Advance(length);
                }

                if (type == 1 || type == 4) // FULL or LAST.
                {
                    action.Invoke(m_record);
                    m_record = new Record();
                }

                return true;
            }
        }
    }
}
