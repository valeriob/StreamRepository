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
        Position _commitPosition = Position.Start;

        byte[] _lastPage = new byte[PageSize];
        Page _lastPage2;

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

            using (var stream = _blob.OpenRead())
            {
                stream.Seek(_commitPosition.Page * PageSize, SeekOrigin.Begin);
                stream.Read(_lastPage, 0, 512);
            }

            _lastPage2 = new Page(_commitPosition, _lastPage);
        }


        public void Append(byte[] buffer, int start, int count)
        {
            Ensure_There_Is_Space_For(count, true);

            int offset = _commitPosition.Offset;
            int page = _commitPosition.Page;

            int free = Math.Abs(PageSize - offset);
            int copied = start;
            int plusPage = 0;

            using (var payload = new MemoryStream())
            {
                if (offset == 0 && count < PageSize || (free > 0 && free < PageSize))
                {
                    int toCopy = Math.Min(free, count);

                    Array.Copy(buffer, start, _lastPage, offset, toCopy);

                    //_blob.WritePagesAsync(_lastPage, 0, PageSize, _lastPageIndex * PageSize);

                    using (var stream = new MemoryStream(_lastPage, 0, PageSize))
                        _blob.WritePages(stream, page * PageSize);

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
                        _blob.WritePages(stream, (page + plusPage) * PageSize);

                    //payload.Write(buffer, start + copied, count - copied - rem);
                }


                if (rem > 0)
                {
                    var lastPage = new byte[PageSize];
                    Array.Copy(buffer, start + count - rem, lastPage, 0, rem);

                    //_blob.WritePagesAsync(lastPage, 0, PageSize, (_lastPageIndex + fullPages + plusPage) * PageSize);

                    using (var stream = new MemoryStream(lastPage))
                        _blob.WritePages(stream, (page + fullPages + plusPage) * PageSize);

                    // payload.Write(lastPage, 0, PageSize);
                }

                //payload.Seek(0, SeekOrigin.Begin);
                //_blob.WritePages(payload, _lastPageIndex * PageSize);
               // await _blob.WritePagesAsync(payload, _lastPageIndex * PageSize);
            }

            Commit_Position(page * PageSize + offset + count);

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
            return new Position(length);
        }

        void Commit_Position(int length)
        {
            _blob.Metadata[Metadata_Size] = length +"";
            _blob.SetMetadata();

            _commitPosition = new Position(length);
        }

       
    }


    public struct Position
    {
        public readonly static Position Start = new Position(0);

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

        public Position(int length)
        {
            _page = Math.DivRem(length, PageBlobState.PageSize, out _offset);
        }


        public int ToLinearAddress()
        {
            return Page * PageBlobState.PageSize + Offset;
        }
    }


    public class Page
    {
        public Position Position { get; private set; }
        public byte[] _data { get; set; }

        public Page(Position position, byte[] data)
        {
            Position = position;
            _data = data;
        }

        public Page(int page, int offset, byte[] data) : this(new Position(page, offset), data) 
        {
        }

        public Page(int position, byte[] data) : this(new Position(position), data) 
        { 
        }

        public int Free_Space()
        {
            return PageBlobState.PageSize - Position.Offset;
        }
    }
}
