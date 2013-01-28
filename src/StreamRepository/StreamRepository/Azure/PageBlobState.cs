using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace StreamRepository.Azure
{
    public class PageBlobState
    {
        CloudPageBlob _blob;
        CloudPageBlob _index;
        public const Int16 PageSize = 512;
        int _lastPageIndex;
        int _offset;
        byte[] _lastPage = new byte[PageSize];


        public PageBlobState(CloudBlobDirectory directory, string name)
        {
            _blob = directory.GetPageBlobReference(name);
            if (!_blob.Exists())
                _blob.Create(0);
            _index = directory.GetPageBlobReference(name + "-index");
            if (!_index.Exists())
                _index.Create(PageSize);

            using (var stream = _index.OpenRead())
            {
                var tmp = new byte[4];
                stream.Read(tmp, 0, 4);
                int length = BitConverter.ToInt32(tmp, 0);
                _lastPageIndex = Math.DivRem(length, PageSize, out _offset);
            }

            _blob.FetchAttributes();
            using (var stream = _blob.OpenRead())
            {
                stream.Seek(_lastPageIndex * PageSize, SeekOrigin.Begin);
                stream.Read(_lastPage, 0, 512);
            }
        }


        public async void Append(byte[] buffer, int start, int count)
        {
            Ensure_There_Is_Space_For(count, true);

            int free = Math.Abs(PageSize - _offset);
            int copied = start;
            int plusPage = 0;

            using (var payload = new MemoryStream())
            {
                if (_offset == 0 && count < PageSize || (free > 0 && free < PageSize))
                {
                    int toCopy = Math.Min(free, count);

                    Array.Copy(buffer, start, _lastPage, _offset, toCopy);

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
                    using (var stream = new MemoryStream(buffer, start + copied, count - copied - rem))
                        _blob.WritePages(stream, (_lastPageIndex + plusPage) * PageSize);
                    //payload.Write(buffer, start + copied, count - copied - rem);
                }


                if (rem > 0)
                {
                    var lastPage = new byte[PageSize];
                    Array.Copy(buffer, start + count - rem, lastPage, 0, rem);
                    using (var stream = new MemoryStream(lastPage))
                        _blob.WritePages(stream, (_lastPageIndex + fullPages + plusPage) * PageSize);
                    // payload.Write(lastPage, 0, PageSize);
                }

                //payload.Seek(0, SeekOrigin.Begin);
                //_blob.WritePages(payload, _lastPageIndex * PageSize);
            }

            int offset;
            int lastPageIndex = Math.DivRem(_lastPageIndex * PageSize + _offset + count, PageSize, out offset);

            var page = new byte[PageSize];
            var bytes = BitConverter.GetBytes(lastPageIndex * PageSize + offset);
            Array.Copy(bytes, page, 4);

            //using (var stream = new MemoryStream(page))
            //    _index.WritePages(stream, 0);

            var indexStream = new MemoryStream(page);
            IAsyncResult ar = null;
            ar = _index.BeginWritePages(indexStream, 0, null, state =>
            {
                _index.EndWritePages(ar);
                indexStream.Dispose();
            }, null);

            _lastPageIndex = lastPageIndex;
            _offset = offset;
        }


        public IEnumerable<RecordValue> Read_Values()
        {
            using (var stream = new MemoryStream())
            {
                _blob.DownloadToStream(stream);
                stream.Seek(0, SeekOrigin.Begin);

                while (stream.Position < Current_Position())
                    yield return FramedValue.Deserialize(stream, stream.Position);
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
                neededSize += Current_Position();

            if (_blob.Properties.Length < neededSize)
                _blob.Resize(neededSize);
        }


        public int Current_Position()
        {
            return _lastPageIndex * PageSize + _offset;
        }
    }

}
