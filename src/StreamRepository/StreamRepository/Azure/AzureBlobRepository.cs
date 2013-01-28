using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.Azure
{
    public class AzureBlobRepository : Repository
    {
        CloudBlobDirectory _directory;
        Func<int, string> _logFileName = year => string.Format("{0}", year);
        Dictionary<int, PageBlobState> _cache;

        public AzureBlobRepository(CloudBlobDirectory directory)
        {
            _directory = directory;
            _cache = new Dictionary<int, PageBlobState>();
        }

        public override async Task Append_Values(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            foreach (var year in values.GroupBy(g => g.Item1.Year))
            {
                using (var stream = new MemoryStream())
                {
                    using (var writer = new BinaryWriter(stream))
                        foreach (var value in year)
                        {
                            var framed = new FramedValue(value.Item1, value.Item2, value.Item3);
                            framed.Serialize(writer);
                        }

                    int writtenBytes = year.Count() * FramedValue.SizeInBytes();

                    var blob = Open(year.Key);
                    blob.Append(stream.GetBuffer(), 0, writtenBytes);
                }
            }
        }

        public override IEnumerable<RecordValue> Get_Values()
        {
            foreach (var year in Get_Years())
            {
                var blob = Open(year);
                foreach (var value in blob.Read_Values())
                    yield return value;
            }
        }

        //public override async Task Append_Values(IEnumerable<Tuple<DateTime, double, int>> values)
        //{
        //    foreach (var year in values.GroupBy(g => g.Item1.Year))
        //    {
        //        using (var stream = Open_Stream_For_Writing(year.Key))
        //        {
        //            using (var buffer = new MemoryStream())
        //            {
        //                using (var writer = new BinaryWriter(buffer))
        //                    foreach (var value in year)
        //                    {
        //                        var framed = new FramedValue(value.Item1, value.Item2, value.Item3);
        //                        framed.Serialize(writer);
        //                    }

        //                int writtenBytes = year.Count() * FramedValue.SizeInBytes();
        //                stream.Write(buffer.GetBuffer(), 0, writtenBytes);
        //            }
        //        }
        //    }
        //}

        //public override IEnumerable<RecordValue> Get_Values()
        //{
        //    foreach (var year in Get_Years())
        //    {
        //        CloudPageBlob blob = _directory.GetPageBlobReference(_logFileName(year));
        //        Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob b;
               
        //        using(var buffer = new MemoryStream())
        //        {
        //            blob.DownloadToStream(buffer);
        //            buffer.Seek(0, SeekOrigin.Begin);

        //            using (var stream = new Lokad.Cqrs.TapeStorage.PageReadStream(buffer))
        //            {
        //                var reader = new BinaryReader(stream);

        //                while (stream.Position < stream.Length)
        //                    yield return FramedValue.Deserialize(reader, stream.Position);
        //            }
        //        }
        //    }
        //}

        public override IEnumerable<RecordValue> Get_Values(DateTime from)
        {
            foreach (var year in Get_Years().Where(y => y >= from.Year))
            {
                var blob = new PageBlobState(_directory, _logFileName(year));
                foreach (var value in blob.Read_Values())
                    yield return value;
            }
        }

        protected override Stream Open_Stream_For_Reading(int year)
        {
            throw new NotImplementedException();
            //return Create_ReadOnly_Index_Stream(_logFileName(year));
        }
        protected override Stream Open_Stream_For_Writing(int year)
        {
            throw new NotImplementedException();
            //return Create_ReadWrite_Index_Stream(_logFileName(year));
        }

        protected override IEnumerable<int> Get_Years()
        {
            foreach (var blob in _directory.ListBlobs())
            {
                var last = blob.Uri.Segments.Last();
                int year;
                if (Int32.TryParse(last, out year))
                    yield return year;
            }
        }

        public override void Hint_Year_Samples(int year, int samples)
        {
            Open(year).Ensure_There_Is_Space_For(samples * FramedValue.SizeInBytes());
        }

        //Stream Create_ReadOnly_Index_Stream(string name)
        //{
        //    var blob = _directory.GetPageBlobReference(name);
        //    if(!blob.Exists())
        //        blob.Create(0);
        //    var stream = new Lokad.Cqrs.TapeStorage.PageBlobReadStream(blob);
        //    return stream;
        //}

        //Stream Create_ReadWrite_Index_Stream(string name)
        //{
        //    var blob = _directory.GetPageBlobReference(name);
        //    if (!blob.Exists())
        //        blob.Create(0);
        //    var stream = new Lokad.Cqrs.TapeStorage.PageBlobAppendStream(blob);
        //    return stream;
        //}

        PageBlobState Open(int year)
        {
            PageBlobState blob;
            if (!_cache.TryGetValue(year, out blob))
            {
                blob = new PageBlobState(_directory, _logFileName(year));
                _cache[year] = blob;
            }
            return blob;
        }
        void Append_To_PageBlob(CloudPageBlob blob, byte[] buffer, int start, int count)
        {
           // blob.WritePages()
        }
    }


}
