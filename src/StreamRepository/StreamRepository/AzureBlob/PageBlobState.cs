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
    public class AzurePageBlob
    {
        public static readonly string Metadata_Size = "Size";
        public static readonly Int16 PageSize = 512;
        Position _commitPosition = Position.Start;
        
        CloudPageBlob _blob;
        Page _lastPage;


        public AzurePageBlob(CloudPageBlob blob)
        {
            _blob = blob;
        }


        public async Task OpenAsync()
        {
            try
            {
                await _blob.FetchAttributesAsync();
                _commitPosition = Get_Committed_Position();
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404)
                    throw;

                _commitPosition = Position.Start;
            }

            byte[] lastPage = new byte[PageSize];
            if (_commitPosition.ToLinearAddress() != 0)
            {
                using (var stream = _blob.OpenRead())
                {
                    stream.Seek(_commitPosition.Page * PageSize, SeekOrigin.Begin);
                    await stream.ReadAsync(lastPage, 0, 512);
                }
            }

            _lastPage = new Page(_commitPosition, lastPage);
        }

        public void Append(string text)
        {
            var buffer = Encoding.UTF8.GetBytes(text);
            Append(buffer, 0, buffer.Length);
        }

        public void Append(byte[] buffer, int start, int count)
        {
            WriteAt(_commitPosition.ToLinearAddress(), buffer, start, count).Wait();
        }

        public Task AppendAsync(Stream stream)
        {
            return WriteAtAsync(_commitPosition.ToLinearAddress(), stream);
        }

        public Task WriteAt(int position, byte[] buffer, int start, int count)
        {
            return WriteAtAsync(position, new MemoryStream(buffer, start, count));
        }

        public async Task WriteAtAsync(int position, Stream stream)
        {
            try
            {
                await Ensure_There_Is_Space_For_More((int)stream.Length);
            }
            catch(StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode != 404 || position != 0)
                    throw;
            }

            var prepare = await PreparePages(position, stream);

            if (prepare.Position == 0)
                await _blob.UploadFromStreamAsync(prepare.Pages);
            else
                await _blob.WritePagesAsync(prepare.Pages, prepare.Position);

            await Commit_new_length_if_it_grew(prepare.NewLength(), prepare.LastPage);
        }

        async Task<PreparedPages> PreparePages(int position, Stream stream)
        {
            int count = (int)stream.Length;
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
                        await lastFilling;
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

            return new PreparedPages
            {
                Position = position,
                Pages = bufferedPages,
                LastPage = lastPage
            };
        }

        async Task Commit_new_length_if_it_grew(long newLength, Page lastPage)
        {
            if (_commitPosition.ToLinearAddress() < newLength)
            {
                await Commit_Position(newLength);
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

        public async Task<Stream> DownloadValuesAsync()
        {
            var stream = new BufferPoolStream(new BufferPool());

            await _blob.DownloadRangeToStreamAsync(stream, 0, _commitPosition.ToLinearAddress());
            
            stream.Seek(0, SeekOrigin.Begin);

            return stream;
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

        public IEnumerable<BufferedRecord> Read_Raw_Values2()
        {
            var pool = new BufferPool();
            using (var stream = new BufferPoolStream(pool))
            {
                _blob.DownloadRangeToStream(stream, 0, _commitPosition.ToLinearAddress());
                stream.Seek(0, SeekOrigin.Begin);
                
                while (stream.Position < _commitPosition.ToLinearAddress())
                    yield return new BufferedRecord(pool, stream.Position);
            }
        }

        public async Task Ensure_There_Is_Space_For(int lenght)
        {
            if (_blob.Properties.Length != 0)
                throw new Exception("Already initialize");
                
            await _blob.ResizeAsync(lenght);
        }

        public async Task Ensure_There_Is_Space_For_More(int lenght)
        {
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
                    await _blob.ResizeAsync(neededSize);
                }
                catch (StorageException ex)
                {
                    if (ex.RequestInformation.HttpStatusCode == 404)
                        return;
                    throw;
                }
            }
        }



        Position Get_Committed_Position()
        {
            var length = Get_Committed_Length_For(_blob);
            return new Position(length);
        }

        async Task Commit_Position(long length)
        {
            await Commit_Length_For(_blob, length);
            _commitPosition = new Position(length);
        }

        int Get_Committed_Length_For(CloudPageBlob blob)
        {
            if (!blob.Metadata.ContainsKey(Metadata_Size))
                return 0;

            return int.Parse(_blob.Metadata[Metadata_Size]);
        }

        async Task Commit_Length_For(CloudPageBlob blob, long length)
        {
            blob.Metadata[Metadata_Size] = length + "";
            await blob.SetMetadataAsync();
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


        class PreparedPages
        {
            public int Position { get; set; }
            public StreamJoiner Pages { get; set; }
            public Page LastPage { get; set; }

            public long NewLength()
            {
                return Position + Pages.Length;
            }
        }
    }


    public class BufferedRecord
    {
        BufferPool _pool;
        long _position;

        public BufferedRecord(BufferPool pool, long position)
        {
            _pool = pool;
            _position = position;

            new BufferPoolStream(_pool);
        }
    }
}
