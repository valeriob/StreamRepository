using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Diagnostics;

namespace Test
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            var s1 = new MemoryStream();
            var s2 = new MemoryStream();
            s1.WriteByte(1);
            s2.WriteByte(2);

            var j = new StreamJoiner(s1, s2);

            var buffer = new byte[12];
            int read = j.Read(buffer, 0, 2);
        }
    }

    public class StreamJoiner : Stream
    {
        long _length;
        long _position;

        Stream[] _chain;
        public StreamJoiner(params Stream[] chain)
        {
            if (chain == null)
                throw new ArgumentNullException("chain");

            _chain = chain;
            _length = chain.Sum(l => l.Length);

            foreach (var stream in chain)
                stream.Seek(0, SeekOrigin.Begin);
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
            long left = _length - _position;
            int read = 0;
            int index = -1;

            do
            {
                index++;
                var current =_chain[index];

                int toRead = (int)Math.Min(left, current.Length);
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
    }
}
