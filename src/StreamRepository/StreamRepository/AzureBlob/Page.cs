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
        int _index;
        int _offset;
        byte[] _data;

        public Page(Position position, byte[] data)
        {
            if (data == null || data.Length != AzurePageBlob.PageSize)
                throw new Exception("page data must be 512 in size");

            _index = position.Page;
            _offset = position.Offset;
            _data = data;
        }

        public Page(int position)
            : this(new Position(position), new byte[AzurePageBlob.PageSize])
        {
        }


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
                stream.Read(_data, 0, AzurePageBlob.PageSize);
            }
        }
        public async Task FillFromBlobAsync(CloudPageBlob blob)
        {
            using (var stream = blob.OpenRead())
            {
                stream.Seek(this.GetBaseAddress(), SeekOrigin.Begin);
                await stream.ReadAsync(_data, 0, AzurePageBlob.PageSize);
            }
        }

        public int Free_Space()
        {
            return AzurePageBlob.PageSize - _offset;
        }
        public bool IsEmpty()
        {
            return _offset == 0;
        }

        public bool IsFull()
        {
            return _offset == AzurePageBlob.PageSize;
        }

        public bool Is_empty_and_can_contain_all_data___or___Is_not_empty(int count)
        {
            return (IsEmpty() && count < AzurePageBlob.PageSize) || Free_Space() < AzurePageBlob.PageSize;
        }




        public int Fill_From(byte[] buffer, int start, int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException("count");

            int toCopy = Math.Min(AzurePageBlob.PageSize, count);

            Array.Copy(buffer, start, _data, 0, toCopy);

            _offset += toCopy;

            return toCopy;
        }

        public int Fill_From(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");


            int toCopy = (int)Math.Min(AzurePageBlob.PageSize, stream.Length - stream.Position);

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
            return _index * AzurePageBlob.PageSize;
        }

        public override string ToString()
        {
            return string.Format("{0}", new Position(_index, _offset));
        }
    }
}
