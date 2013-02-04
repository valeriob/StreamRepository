
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    // startint position to be implemented
    public class StreamSegment : Stream
    {
        int _length;
        int _start;
        Stream _innerStream;

        public StreamSegment(Stream innerStream, int start, int length)
        {
            _innerStream = innerStream;
            _start = start;
            _length = length;
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
                return _innerStream.Position;
            }
            set
            {
                _innerStream.Position = value;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int limited = (int)Math.Min(count, _length - _innerStream.Position);
            _innerStream.Seek(_start, SeekOrigin.Begin);
            int read = _innerStream.Read(buffer, offset, limited);
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _innerStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
