using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public class StreamJoiner : Stream
    {
        long _length;
        long _position;
        List<StreamChunk> _chunks;


        public StreamJoiner()
        {
            _chunks = new List<StreamChunk>();
        }


        public void Append(Stream stream, int start = 0, int? count = null)
        {
            if (stream.Length < start + count)
                throw new ArgumentOutOfRangeException("");
            if (!stream.CanSeek && start != 0)
                throw new ArgumentException();

            if (count == null)
                count = (int)stream.Length;

            _chunks.Add(new StreamChunk(stream, start, count.Value));

            _length = _chunks.Sum(c => c.Count);
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
        }

        public override long Length
        {
            get { return _length; }
        }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                _position = value;
            }
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }


        public override int Read(byte[] buffer, int offset, int count)
        {
            long left = Math.Min(_length - _position, count);
            int read = 0;

            do
            {
                var p = GetPosition(_position + read);
                var chunk = _chunks[p.Index];
                var stream = chunk.Stream;
                long localLeft = chunk.Count - p.Offset;

                stream.Seek(chunk.Start + p.Offset, SeekOrigin.Begin);
              
                int toRead = Min(left, localLeft, buffer.Length);
                int test = stream.Read(buffer, offset + read, toRead);
                Debug.Assert(test == toRead);

                read += toRead;
                left -= toRead;

            } while (left > 0);

            _position = _position + read;
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }
  

        int Min(params long[] values)
        {
            var min = long.MaxValue;
            foreach (var value in values)
                min = Math.Min(min, value);
            return (int)min;
        }

        Position GetPosition(long length)
        {
            int index = 0;
            int tmp = 0;
            int consumed = 0;

            for (int i = 0; i < _chunks.Count && tmp <= length; i++)
            {
                tmp += _chunks[i].Count;
                if (i > 0)
                    consumed += _chunks[i - 1].Count;
                index = i;
            }

            int offset = (int)length - consumed;
            return new Position(index, offset);
        }

    }

    struct Position
    {
        public readonly int Index;
        public readonly int Offset;

        public Position(int index, int offset)
        {
            Index = index;
            Offset = offset;
        }
    }

    class StreamChunk
    {
        public Stream Stream { get; private set; }
        public int Start { get; private set; }
        public int Count { get; private set; }


        public StreamChunk(Stream stream, int start, int count)
        {
            Stream = stream;
            Start = start;
            Count = count;
        }

    }
}
