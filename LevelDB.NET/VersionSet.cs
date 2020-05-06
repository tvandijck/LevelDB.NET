using System;
using System.Collections.Generic;
using System.Text;

namespace LevelDB.NET
{
    internal class VersionSet : IDisposable
    {
        private bool m_hasComparator;
        private string m_comparator = string.Empty;

        private bool m_hasLogNumber;
        private ulong m_logNumber;

        private bool m_hasNextFileNumber;
        private ulong m_nextFileNumber;

        private bool m_hasLastSequence;
        private ulong m_lastSequence;

        private bool m_hasPrevLogNumber;
        private ulong m_prevLogNumber;

        private readonly List<TableFile> m_files = new List<TableFile>();

        private enum Tag
        {
            kComparator = 1,
            kLogNumber = 2,
            kNextFileNumber = 3,
            kLastSequence = 4,
            kCompactPointer = 5,
            kDeletedFile = 6,
            kNewFile = 7,
            // 8 was used for large value refs
            kPrevLogNumber = 9
        };


        public void Dispose()
        {
            foreach (var file in m_files)
            {
                file.Dispose();
            }
            m_files.Clear();
        }

        public void Add(Record record)
        {
            var actionDict = new Action<Slice>[]
            {
                    Error,
                    ReadComparator,
                    ReadLogNumber,
                    ReadNextFileNumber,
                    ReadLastSequence,
                    ReadCompactPointer,
                    ReadDeletedFiled,
                    ReadNewFile,
                    Error,
                    ReadPrevLogNumber,
            };

            var slice = new Slice(record.Bytes);
            while (slice.Length > 0)
            {
                var type = Coding.DecodeVarint32(slice);
                actionDict[type].Invoke(slice);
            }
        }

        public bool HasComparator
        {
            get { return m_hasComparator; }
        }

        public string Comparator
        {
            get { return m_comparator; }
        }

        public bool HasLogNumber
        {
            get { return m_hasLogNumber; }
        }

        public ulong LogNumber
        {
            get { return m_logNumber; }
        }

        public bool HasNextFileNumber
        {
            get { return m_hasNextFileNumber; }
        }

        public ulong NextFileNumber
        {
            get { return m_nextFileNumber; }
        }

        public bool HasLastSequence
        {
            get { return m_hasLastSequence; }
        }

        public ulong LastSequence
        {
            get { return m_lastSequence; }
        }

        public bool HasPrevLogNumber
        {
            get { return m_hasPrevLogNumber; }
        }

        public ulong PrevLogNumber
        {
            get { return m_prevLogNumber; }
        }

        public IEnumerable<TableFile> Files
        {
            get { return m_files.AsReadOnly(); }
        }

        private void Error(Slice slice)
        {
            throw new Exception("Unknown record type field.");
        }

        private void ReadComparator(Slice slice)
        {
            var bytes = Coding.DecodeLengthPrefixed(slice);
            m_comparator = Encoding.ASCII.GetString(bytes);
            m_hasComparator = true;
        }

        private void ReadLogNumber(Slice slice)
        {
            m_logNumber = Coding.DecodeVarint64(slice);
            m_hasLogNumber = true;
        }

        private void ReadNextFileNumber(Slice slice)
        {
            m_nextFileNumber = Coding.DecodeVarint64(slice);
            m_hasNextFileNumber = true;
        }

        private void ReadLastSequence(Slice slice)
        {
            m_lastSequence = Coding.DecodeVarint64(slice);
            m_hasLastSequence = true;
        }

        private void ReadCompactPointer(Slice slice)
        {
            var level = Coding.DecodeVarint32(slice);
            var pointer = Coding.DecodeLengthPrefixed(slice);
        }

        private void ReadDeletedFiled(Slice slice)
        {
            var level = Coding.DecodeVarint32(slice);
            var fileNr = Coding.DecodeVarint64(slice);

        }

        private void ReadNewFile(Slice slice)
        {
            var level = Coding.DecodeVarint32(slice);
            var fileNr = Coding.DecodeVarint64(slice);
            var fileSize = Coding.DecodeVarint64(slice);
            var smallest = new Key(Coding.DecodeLengthPrefixed(slice));
            var largest = new Key(Coding.DecodeLengthPrefixed(slice));

            m_files.Add(new TableFile(level, fileNr, fileSize, smallest, largest));
        }

        private void ReadPrevLogNumber(Slice slice)
        {
            m_prevLogNumber = Coding.DecodeVarint64(slice);
            m_hasPrevLogNumber = true;
        }
    }
}
