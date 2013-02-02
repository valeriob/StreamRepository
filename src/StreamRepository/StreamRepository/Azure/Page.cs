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
        //public Position Position { get; private set; }
        int _index;
        int _offset;
        byte[] _data;

        public Page(Position position, byte[] data)
        {
            if (data == null || data.Length != PageBlobState.PageSize)
                throw new Exception("page data must be 512 in size");

            //Position = position;
            _index = position.Page;
            _offset = position.Offset;
            _data = data;
        }
        
        //public Page(Position position)
        //    : this(position, new byte[PageBlobState.PageSize])
        //{
        //}

        //public Page(int page, int offset, byte[] data)
        //    : this(new Position(page, offset), data)
        //{
        //}

        //public Page(int position, byte[] data)
        //    : this(new Position(position), data)
        //{
        //}
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
                blob.WritePages(stream, GetBaseAddress());
        }

        public void FillFromBlob(CloudPageBlob blob)
        {
            using (var stream = blob.OpenRead())
            {
                stream.Seek(this.GetBaseAddress(), SeekOrigin.Begin);
                stream.Read(_data, 0, PageBlobState.PageSize);
            }
        }
        public async Task FillFromBlobAsync(CloudPageBlob blob)
        {
            using (var stream = blob.OpenRead())
            {
                stream.Seek(this.GetBaseAddress(), SeekOrigin.Begin);
                await stream.ReadAsync(_data, 0, PageBlobState.PageSize);
            }
        }

        public int Free_Space()
        {
            return PageBlobState.PageSize - _offset;
        }
        public bool IsEmpty()
        {
            return _offset == 0;
        }

        public bool IsFull()
        {
            return _offset == PageBlobState.PageSize;
        }

        public bool Is_empty_and_can_contain_all_data___or___Is_not_empty(int count)
        {
            return (IsEmpty() && count < PageBlobState.PageSize) || Free_Space() < PageBlobState.PageSize;
        }




        public int Fill_From(byte[] buffer, int start, int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException("count");

            int toCopy = Math.Min(PageBlobState.PageSize, count);

            Array.Copy(buffer, start, _data, 0, toCopy);

            _offset += toCopy;

            return toCopy;
        }

        public int Fill_From(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");


            int toCopy = (int)Math.Min(PageBlobState.PageSize, stream.Length - stream.Position);

            stream.Read(_data, 0, toCopy);

            _offset += toCopy;

            return toCopy;
        }

        public int Append(byte[] buffer, int start, int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException("count");

            int toCopy = Math.Min(Free_Space(), count);

            Array.Copy(buffer, start, _data, _offset, toCopy);

            _offset += toCopy;

            return toCopy;
        }
        public int Append(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            int toCopy = (int)Math.Min(Free_Space(), stream.Length - stream.Position);

            stream.Read(_data, _offset, toCopy);

            _offset += toCopy;

            return toCopy;
        }

        public Page Clone()
        {
            return new Page(new Position(_index, _offset), _data);
        }

        public int GetBaseAddress()
        {
            return _index * PageBlobState.PageSize;
        }

        public override string ToString()
        {
            return string.Format("{0}", new Position(_index, _offset));
        }
    }
}
