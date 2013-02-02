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

        List<Stream> _chain = new List<Stream>();
        List<int> _starts = new List<int>();
        List<int> _counts = new List<int>();
        //public StreamJoiner(params Stream[] chain)
        //{
        //    if (chain == null)
        //        throw new ArgumentNullException("chain");

        //    _chain = chain.ToList();
        //    _length = chain.Sum(l => l.Length);

        //    foreach (var stream in chain)
        //        stream.Seek(0, SeekOrigin.Begin);
        //}

        public void Append(Stream stream, int start = 0, int? count = null)
        {
            if (stream.Length < start + count)
                throw new ArgumentOutOfRangeException("");
            if (!stream.CanSeek && start != 0)
                throw new ArgumentException();

            _chain.Add(stream);
            _starts.Add(start);
            if (count != null)
                _counts.Add(count.Value);
            else
                _counts.Add((int)stream.Length);


            
            //_length = _chain.Sum(l => l.Length);
            _length = _counts.Sum();
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
                var current = _chain[p.Index];
                long localLeft = _counts[p.Index] - p.Offset;

                current.Seek(_starts[p.Index] + p.Offset, SeekOrigin.Begin);
              
                int toRead = Min(left, localLeft, buffer.Length);
                int test = current.Read(buffer, offset + read, toRead);
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
            for (int i = 0; i < _chain.Count && tmp <= length; i++)
            {
                tmp += _counts[i];
                if (i > 0)
                    consumed += _counts[i - 1];
                index = i;
            }
            int offset = (int)length - consumed;
            return new Position(index, offset);
        }

    }

    public struct Position
    {
        public readonly int Index;
        public readonly int Offset;

        public Position(int index, int offset)
        {
            Index = index;
            Offset = offset;
        }
    }
}
