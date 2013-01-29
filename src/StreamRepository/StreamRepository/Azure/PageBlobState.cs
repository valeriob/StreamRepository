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
        Page _lastPage;


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


            byte[] lastPage = new byte[PageSize];
            using (var stream = _blob.OpenRead())
            {
                stream.Seek(_commitPosition.Page * PageSize, SeekOrigin.Begin);
                stream.Read(lastPage, 0, 512);
            }

            _lastPage = new Page(_commitPosition, lastPage);
        }


        public void Append(byte[] buffer, int start, int count)
        {
            Ensure_There_Is_Space_For(count, true);

            int page = _commitPosition.Page;

            int copied = start;
            int plusPage = 0;

            Page lastPage = _lastPage.Clone();

            if (lastPage.Is_empty_and_can_contain_all_data___or___Is_not_empty(count))
            {
                copied = lastPage.Fill(buffer, start, count);

                using (var stream = lastPage.ToStream())
                    _blob.WritePages(stream, _commitPosition.ToPageAddress());

                plusPage = 1;
            }


            int rem;
            int fullPages = Math.DivRem(count - copied, PageSize, out rem);

            if (fullPages > 0)
            {
                using (var stream = new MemoryStream(buffer, start + copied, count - copied - rem))
                    _blob.WritePages(stream, (page + plusPage) * PageSize);

                lastPage = new Page((page + plusPage + fullPages) * PageSize);
            }


            if (rem > 0)
            {
                int currentPosition = (page + fullPages + plusPage) * PageSize;

                lastPage = new Page(currentPosition);
                lastPage.Fill(buffer, start + count - rem, rem);
                lastPage.WriteToBlob(_blob);
            }

            Commit_Position(_commitPosition.ToLinearAddress() + count);

            _lastPage = lastPage;
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

}
