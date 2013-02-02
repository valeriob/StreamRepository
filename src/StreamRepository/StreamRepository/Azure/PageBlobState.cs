using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using EventStore.BufferManagement;
using System.Diagnostics;

namespace StreamRepository.Azure
{
    public class PageBlobState
    {
        public static readonly string Metadata_Size = "Size";
        public static readonly Int16 PageSize = 512;
        CloudPageBlob _blob;
        bool _isNew;
        Position _commitPosition = Position.Start;
        Page _lastPage;


        public PageBlobState(CloudPageBlob blob)
        {
            _blob = blob;
        }


        public void Open()
        {
            try
            {
                _blob.FetchAttributes();
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404)
                    throw;
                _isNew = true;
            }

            _commitPosition = Get_Committed_Position();

            byte[] lastPage = new byte[PageSize];
            if (_commitPosition.ToLinearAddress() != 0)
            {
                using (var stream = _blob.OpenRead())
                {
                    stream.Seek(_commitPosition.Page * PageSize, SeekOrigin.Begin);
                    stream.Read(lastPage, 0, 512);
                }
            }

            _lastPage = new Page(_commitPosition, lastPage);
        }

        public void Create_if_does_not_exists()
        {
            if (_blob.Exists())
                return;
            _blob.Create(PageSize * 128);
            _blob.Metadata[Metadata_Size] = "0";
            //_blob.SetMetadata();
        }


        public void Append(byte[] buffer, int start, int count)
        {
            WriteAt(_commitPosition.ToLinearAddress(), buffer, start, count);
        }

        public void Append(string text)
        {
            var buffer = Encoding.UTF8.GetBytes(text);
            Append(buffer, 0, buffer.Length);
        }

        public void Append(Stream stream)
        {
            WriteAt(_commitPosition.ToLinearAddress(), stream);
        }

        public void WriteAt(int position, byte[] buffer, int start, int count)
        {
            WriteAt(position, new MemoryStream(buffer, start, count));
        }

        public void WriteAt(int position, Stream stream)
        {
            var count = (int)stream.Length;
            Ensure_There_Is_Space_For_More(count);

            int copied = 0;
            int currentPosition = position;
            int firstPageNumber = position / PageSize;
            int lastPageNumber = ((position + count) / PageSize);

            Page firstPage = Retrieve_Page_At(position);
            Page lastPage = new Page(position + count);
            Task lastFilling = null;


            if (firstPageNumber == lastPageNumber)
                lastPage = firstPage;
            else
                if (lastPage.GetBaseAddress() < _commitPosition.ToLinearAddress())
                    lastFilling = lastPage.FillFromBlobAsync(_blob);


            var bufferedPages = new StreamJoiner();

            if (firstPage.Is_empty_and_can_contain_all_data___or___Is_not_empty(count))
            {
                copied = firstPage.Append(stream);

                bufferedPages.Append(firstPage.ToStream());

                currentPosition = position + copied;
            }

            if (copied < count)
            {
                int rem;
                int fullPages = Math.DivRem(count - copied, PageSize, out rem);

                if (fullPages > 0)
                {
                    bufferedPages.Append(stream, copied, count - copied - rem);

                    copied += fullPages * PageSize;
                    currentPosition = position + copied;

                    stream.Seek(copied, SeekOrigin.Begin);
                }

                if (copied < count)
                {
                    Debug.Assert(rem > 0);

                    if (lastFilling != null)
                        lastFilling.Wait();
                    else
                        lastPage = new Page(position + copied);

                    lastPage.Fill_From(stream);
                    bufferedPages.Append(lastPage.ToStream());

                    copied += rem;
                    currentPosition = position + copied;
                }
            }

            Debug.Assert(position + count == currentPosition);
            Debug.Assert(copied == count);
            Debug.Assert(_isNew && firstPageNumber  == 0);

            if (_isNew)
                _blob.UploadFromStream(bufferedPages);
            else
                _blob.WritePages(bufferedPages, firstPageNumber * PageSize);

            if (_commitPosition.ToLinearAddress() < position + copied)
            {
                Commit_Position(position + copied);
                _lastPage = lastPage;
            }
        }



        public Stream OpenReadonlyStream()
        {
            return new StreamSegment(_blob.OpenRead(), 0, _commitPosition.ToLinearAddress());
        }


        public IEnumerable<RecordValue> Read_Values()
        {
            using (var stream = new BufferPoolStream(new BufferPool()))
            {
                _blob.DownloadRangeToStream(stream, 0, _commitPosition.ToLinearAddress());
                stream.Seek(0, SeekOrigin.Begin);

                while (stream.Position < _commitPosition.ToLinearAddress())
                    yield return FramedValue.Deserialize(stream, stream.Position);
            }
        }

        public IEnumerable<byte[]> Read_Raw_Values()
        {
            var pool = new BufferPool();
            using (var stream = new BufferPoolStream(pool))
            {
                _blob.DownloadRangeToStream(stream, 0, _commitPosition.ToLinearAddress());
                stream.Seek(0, SeekOrigin.Begin);

                while (stream.Position < _commitPosition.ToLinearAddress())
                {
                    var data = new byte[FramedValue.SizeInBytes()];
                    stream.Read(data, 0, data.Length);
                    yield return data;
                }
            }
        }

        public void Ensure_There_Is_Space_For(int lenght)
        {
            if (_blob.Properties.Length != 0)
                throw new Exception("Already initialize");
                
            _blob.Resize(lenght);
        }

        public void Ensure_There_Is_Space_For_More(int lenght)
        {
            return;
            int rem;
            int pages = Math.DivRem(lenght, PageSize, out rem);
            if (rem > 0)
                pages++;

            int growth = 1024 * 1024;
            int neededSize = ((pages * PageSize + _commitPosition.ToLinearAddress()) / growth + 1 ) *growth;

            if (_blob.Properties.Length < neededSize)
            {
                try
                {
                    //var min = Math.Min(neededSize, 1024 * 1024);
                    //var value = Math.Max(min, 1024 * 1024);
                    _blob.Resize(neededSize);
                    //_blob.Resize(neededSize);
                }
                catch (StorageException)
                {
                    _blob.FetchAttributes();
                    _blob.Resize(neededSize);
                }
            }
        }


        Position Get_Committed_Position()
        {
            var length = Get_Committed_Length_For(_blob);
            return new Position(length);
        }

        void Commit_Position(int length)
        {
            Commit_Length_For(_blob, length);
            _commitPosition = new Position(length);
        }



        int Get_Committed_Length_For(CloudPageBlob blob)
        {
            if (!blob.Metadata.ContainsKey(Metadata_Size))
                return 0;

            return int.Parse(_blob.Metadata[Metadata_Size]);
        }

        void Commit_Length_For(CloudPageBlob blob, int length)
        {
            blob.Metadata[Metadata_Size] = length + "";
            blob.SetMetadata();
        }


        Page Retrieve_Page_At(int position)
        {
            if (position == _commitPosition.ToLinearAddress())
                return _lastPage;
            else
            {
                var page = new Page(position);
                page.FillFromBlob(_blob);
                return page;
            }
        }
    }

}
