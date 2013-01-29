using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using EventStore.BufferManagement;

namespace StreamRepository.Azure
{
    public class PageBlobState
    {
        public static readonly string Metadata_Size = "Size";
        public static readonly Int16 PageSize = 512;

        CloudPageBlob _blob;
        CloudPageBlob _index;
        //int _lastPageIndex;
        //int _offset;
        Position _commitPosition = Position.Start;
        byte[] _lastPage = new byte[PageSize];


        public PageBlobState(CloudBlobDirectory directory, string name)
        {
            _blob = directory.GetPageBlobReference(name);
            _index = directory.GetPageBlobReference(name + "-index");
        }


        public void Open()
        {
            if (!_blob.Exists())
            {
                _blob.Create(PageSize * 128);
                _blob.Metadata[Metadata_Size] = "0";
            }
            else
                _blob.FetchAttributes();

            _commitPosition = Get_Committed_Position();
            //int length = Get_Committed_Length();
            //_lastPageIndex = Math.DivRem(length, PageSize, out _offset);

            using (var stream = _blob.OpenRead())
            {
                stream.Seek(_commitPosition.Page * PageSize, SeekOrigin.Begin);
                stream.Read(_lastPage, 0, 512);
            }
        }


        public void Append(byte[] buffer, int start, int count)
        {
            Ensure_There_Is_Space_For(count, true);

            int _offset = _commitPosition.Offset;
            int _lastPageIndex = _commitPosition.Page;

            int free = Math.Abs(PageSize - _offset);
            int copied = start;
            int plusPage = 0;

            using (var payload = new MemoryStream())
            {
                if (_offset == 0 && count < PageSize || (free > 0 && free < PageSize))
                {
                    int toCopy = Math.Min(free, count);

                    Array.Copy(buffer, start, _lastPage, _offset, toCopy);

                    //_blob.WritePagesAsync(_lastPage, 0, PageSize, _lastPageIndex * PageSize);

                    using (var stream = new MemoryStream(_lastPage, 0, PageSize))
                        _blob.WritePages(stream, _lastPageIndex * PageSize);

                    //payload.Write(_lastPage, 0, PageSize);

                    copied = toCopy;
                    plusPage = 1;
                }


                int rem;
                int fullPages = Math.DivRem(count - copied, PageSize, out rem);

                if (fullPages > 0)
                {
                    //_blob.WritePagesAsync(buffer, start + copied, count - copied - rem, (_lastPageIndex + plusPage) * PageSize);

                    using (var stream = new MemoryStream(buffer, start + copied, count - copied - rem))
                        _blob.WritePages(stream, (_lastPageIndex + plusPage) * PageSize);

                    //payload.Write(buffer, start + copied, count - copied - rem);
                }


                if (rem > 0)
                {
                    var lastPage = new byte[PageSize];
                    Array.Copy(buffer, start + count - rem, lastPage, 0, rem);

                    //_blob.WritePagesAsync(lastPage, 0, PageSize, (_lastPageIndex + fullPages + plusPage) * PageSize);

                    using (var stream = new MemoryStream(lastPage))
                        _blob.WritePages(stream, (_lastPageIndex + fullPages + plusPage) * PageSize);

                    // payload.Write(lastPage, 0, PageSize);
                }

                //payload.Seek(0, SeekOrigin.Begin);
                //_blob.WritePages(payload, _lastPageIndex * PageSize);
               // await _blob.WritePagesAsync(payload, _lastPageIndex * PageSize);
            }

            int offset;
            int lastPageIndex = Math.DivRem(_lastPageIndex * PageSize + _offset + count, PageSize, out offset);


            Commit_Position(lastPageIndex * PageSize + offset);

            //var page = new byte[PageSize];
            //var bytes = BitConverter.GetBytes(lastPageIndex * PageSize + offset);
            //Array.Copy(bytes, page, 4);

            //using (var stream = new MemoryStream(page))
            //    _index.WritePages(stream, 0);

            //_index.WritePagesAsync(new MemoryStream(page), 0);
        }


        public IEnumerable<RecordValue> Read_Values()
        {
            using (var stream = new BufferPoolStream(new BufferPool()))
            {
                _blob.DownloadToStream(stream);
                stream.Seek(0, SeekOrigin.Begin);

                while (stream.Position < _commitPosition.ToLinearAddress())
                    yield return FramedValue.Deserialize(stream, stream.Position);
            }
        }

        public IEnumerable<byte[]> Read_Raw_Values()
        {
            using (var stream = new MemoryStream())
            {
                _blob.DownloadToStream(stream);
                stream.Seek(0, SeekOrigin.Begin);

                while (stream.Position < _commitPosition.ToLinearAddress())
                {
                    var data = new byte[FramedValue.SizeInBytes()];
                    stream.Read(data, 0, data.Length);
                    yield return data;
                }
            }
        }

        public void Ensure_There_Is_Space_For(int lenght, bool relative = false)
        {
            int rem;
            int pages = Math.DivRem(lenght, PageSize, out rem);
            if (rem > 0)
                pages++;

            int neededSize = pages * PageSize;
            if (relative)
                neededSize += _commitPosition.ToLinearAddress();

            if (_blob.Properties.Length < neededSize)
            {
                _blob.Resize(neededSize);
            }
        }


        //public int Current_Position()
        //{
        //    return _lastPageIndex * PageSize + _offset;
        //}

        int Get_Committed_Length()
        {
            int length = 0;
            string s = _blob.Metadata[Metadata_Size];
            if (!int.TryParse(_blob.Metadata[Metadata_Size], out length))
                throw new Exception("i could not find the actual size of the blob");

            return length;
        }

        Position Get_Committed_Position()
        {
            var length = Get_Committed_Length();

            int offset;
            int page = Math.DivRem(length, PageSize, out offset);

            return new Position(page, offset);
        }

        void Commit_Position(int length)
        {
            _blob.Metadata[Metadata_Size] = length +"";
            _blob.SetMetadata();

            int offset;
            int page = Math.DivRem(length, PageSize, out offset);
            _commitPosition = new Position(page, offset);
           //_lastPageIndex = Math.DivRem(length, PageSize, out _offset);
        }

       
    }


    public struct Position
    {
        public readonly static Position Start = new Position(0,0);


        private int _page;
        public int Page
        {
            get { return _page; }
        }
        private int _offset;
        public int Offset
        {
            get { return _offset; }
        }
        
  
        public Position(int page, int offset)
        {
            _page = page;
            _offset = offset;
        }


        public int ToLinearAddress()
        {
            return Page * PageBlobState.PageSize + Offset;
        }
    }

}
