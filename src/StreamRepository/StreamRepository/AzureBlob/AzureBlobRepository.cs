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
        Dictionary<string, AzurePageBlob> _blobsCache;
        AzureBlobShardingStrategy _sharding;
        //Cache _cache;

        public AzureBlobRepository(CloudBlobDirectory directory, AzureBlobShardingStrategy sharding)
        {
            _directory = directory;
            _sharding = sharding;
            _blobsCache = new Dictionary<string, AzurePageBlob>();
            //_cache = new FileSystemCache(new DirectoryInfo(""));
        }


        public async Task AppendValues(IEnumerable<Event> values)
        {
            foreach (var shard in _sharding.Shard(values))
            {
                var group = shard.GetValues();
                using (var stream = new BufferPoolStream(new BufferPool()))
                {
                    using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
                        foreach (var value in group)
                            new FramedValue(value.Timestamp, value.Value, value.ImportId).Serialize(writer);

                    int writtenBytes = group.Count() * FramedValue.SizeInBytes();

                    var blob = await OpenBlobAsync(shard.GetName());
                    stream.Seek(0, SeekOrigin.Begin);
                    await blob.AppendAsync(stream);
                }
            }
        }

        public IEnumerable<RecordValue> Get_Values(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(_directory.ListBlobs(), from, to))
            {
                var name = shard.GetName();

                var task = OpenBlobAsync(name);
                task.Wait();

                foreach (var value in task.Result.Read_Values())
                    yield return value;
            }
        }

        public IEnumerable<byte[]> Get_Raw_Values(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(_directory.ListBlobs(), from, to))
            {
                var task = OpenBlobAsync(shard.GetName());
                task.Wait();
                foreach (var value in task.Result.Read_Raw_Values())
                    yield return value;
            }
        }


        public void Hint_Sampling_Period(int samplingPeriodInSeconds)
        {
            // TODO
           // OpenBlobFor(year).Ensure_There_Is_Space_For(samples * FramedValue.SizeInBytes());
        }

        async Task<AzurePageBlob> OpenBlobAsync(string name)
        {
            AzurePageBlob blob;
            if (!_blobsCache.TryGetValue(name, out blob))
            {
                blob = new AzurePageBlob(_directory.GetPageBlobReference(name));
                
                await blob.OpenAsync();

                _blobsCache[name] = blob;
            }
            return blob;
        }

    }

}
