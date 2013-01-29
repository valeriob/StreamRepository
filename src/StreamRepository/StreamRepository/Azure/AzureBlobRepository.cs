using EventStore.BufferManagement;
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

        public AzureBlobRepository(CloudBlobDirectory directory, BufferPool bufferPool)
        {
            _directory = directory;
            _cache = new Dictionary<int, PageBlobState>();
        }


        public override void Append_Values(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            foreach (var year in values.GroupBy(g => g.Item1.Year))
            {
                using (var stream = new MemoryStream())
                {
                    using (var writer = new BinaryWriter(stream))
                        foreach (var value in year)
                            new FramedValue(value.Item1, value.Item2, value.Item3).Serialize(writer);
                        
                    int writtenBytes = year.Count() * FramedValue.SizeInBytes();

                    var blob = OpenBlobFor(year.Key);

                    blob.Append(stream.GetBuffer(), 0, writtenBytes);
                }
            }
        }


        public override IEnumerable<RecordValue> Get_Values(DateTime? from, DateTime? to)
        {
            var years = Get_Years();
            if (from.HasValue)
                years = years.Where(y => y >= from.Value.Year);
            if (to.HasValue)
                years = years.Where(y => y <= to.Value.Year);

            foreach (var year in years)
            {
                var blob = OpenBlobFor(year);
                foreach (var value in blob.Read_Values())
                    yield return value;
            }
        }

        public override  IEnumerable<byte[]> Get_Raw_Values(DateTime? from, DateTime? to)
        {
            var years = Get_Years();
            if (from.HasValue)
                years = years.Where(y => y >= from.Value.Year);
            if (to.HasValue)
                years = years.Where(y => y <= to.Value.Year);

            foreach (var year in years)
            {
                var blob = OpenBlobFor(year);
                foreach (var value in blob.Read_Raw_Values())
                    yield return value;
            }
        }

        IEnumerable<int> Get_Years()
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
            OpenBlobFor(year).Ensure_There_Is_Space_For(samples * FramedValue.SizeInBytes());
        }

        PageBlobState OpenBlobFor(int year)
        {
            PageBlobState blob;
            if (!_cache.TryGetValue(year, out blob))
            {
                blob = new PageBlobState(_directory, _logFileName(year));
                blob.Open();
                _cache[year] = blob;
            }
            return blob;
        }

    }

}
