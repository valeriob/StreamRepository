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
        Dictionary<string, PageBlobState> _cache;
        ShardingStrategy _sharding;


        public AzureBlobRepository(CloudBlobDirectory directory, ShardingStrategy sharding)
        {
            _directory = directory;
            _sharding = sharding;
            _cache = new Dictionary<string, PageBlobState>();
        }

 
        public override void Append_Values(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            foreach (var shard in _sharding.Shard(values))
            {
                var group = shard.GetValues();
                using (var stream = new BufferPoolStream(new BufferPool()))
                {
                    using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
                        foreach (var value in group)
                            new FramedValue(value.Item1, value.Item2, value.Item3).Serialize(writer);

                    int writtenBytes = group.Count() * FramedValue.SizeInBytes();

                    var blob = OpenBlobFor(shard.GetName());
                    stream.Seek(0, SeekOrigin.Begin);
                    blob.Append(stream);
                }
            }
        }

        public override IEnumerable<RecordValue> Get_Values(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(from, to))
            {
                var blob = OpenBlobFor(shard.GetName());
                foreach (var value in blob.Read_Values())
                    yield return value;
            }
        }

        public override  IEnumerable<byte[]> Get_Raw_Values(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(from, to))
            {
                var blob = OpenBlobFor(shard.GetName());
                foreach (var value in blob.Read_Raw_Values())
                    yield return value;
            }
        }


        public override void Hint_Sampling_Period(int samplingPeriodInSeconds)
        {
            // TODO
           // OpenBlobFor(year).Ensure_There_Is_Space_For(samples * FramedValue.SizeInBytes());
        }

        PageBlobState OpenBlobFor(string name)
        {
            PageBlobState blob;
            if (!_cache.TryGetValue(name, out blob))
            {
                blob = new PageBlobState(_directory.GetPageBlobReference(name));
                blob.Create_if_does_not_exists();
                blob.Open();

                _cache[name] = blob;
            }
            return blob;
        }

    }

}
