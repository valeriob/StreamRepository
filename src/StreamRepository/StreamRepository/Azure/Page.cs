using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace StreamRepository.Azure
{
    public class Page
    {
        public Position Position { get; private set; }
        public byte[] _data { get; set; }

        public Page(Position position, byte[] data)
        {
            Position = position;
            _data = data;
        }
        
        public Page(Position position)
            : this(position, new byte[PageBlobState.PageSize])
        {
        }

        public Page(int page, int offset, byte[] data)
            : this(new Position(page, offset), data)
        {
        }

        public Page(int position, byte[] data)
            : this(new Position(position), data)
        {
        }
        public Page(int position)
            : this(new Position(position), new byte[PageBlobState.PageSize])
        {
        }


        //public static Page Create_From_Buffer(byte[] buffer, int start, int count, int position)
        //{
        //    byte[] data = new byte[PageBlobState.PageSize];
        //    Array.Copy(buffer, start, data, 0, count);

        //    return new Page(position, data);
        //}


        public Stream ToStream()
        {
            return new MemoryStream(_data);
        }

        public void WriteToBlob(CloudPageBlob blob)
        {
            using (var stream = ToStream())
                blob.WritePages(stream, Position.ToPageAddress());
        }

        public int Free_Space()
        {
            return PageBlobState.PageSize - Position.Offset;
        }
        public bool IsEmpty()
        {
            return Position.Offset == 0;
        }

        public bool IsFull()
        {
            return Position.Offset == PageBlobState.PageSize;
        }

        public bool Is_empty_and_can_contain_all_data___or___Is_not_empty(int count)
        {
            return (IsEmpty() && count < PageBlobState.PageSize) || Free_Space() < PageBlobState.PageSize;
        }

        public int Fill(byte[] buffer, int start, int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException("count");

            int toCopy = Math.Min(Free_Space(), count);

            Array.Copy(buffer, start, _data, Position.Offset, toCopy);

            Position = Position + toCopy;

            return toCopy;
        }

        public Page Clone()
        {
            return new Page(Position, _data);
        }



        public override string ToString()
        {
            return string.Format("{0}", Position);
        }
    }
}
