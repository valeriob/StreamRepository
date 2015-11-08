using EventStore.BufferManagement;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.Azure.Blob
{
    public class AzureBlobRepository<T> : Repository<T>
    {
        CloudBlobDirectory _directory;
        Func<int, string> _logFileName = year => string.Format("{0}", year);
        Dictionary<string, AzurePageBlob> _blobsCache;
        AzureBlobShardingStrategy<T> _sharding;
        //Cache _cache;
        ISerializeTimeValue<T> _builder;


        public AzureBlobRepository(CloudBlobDirectory directory, AzureBlobShardingStrategy<T> sharding, ISerializeTimeValue<T> builder)
        {
            _directory = directory;
            _sharding = sharding;
            _builder = builder;
            _blobsCache = new Dictionary<string, AzurePageBlob>();
            //_cache = new FileSystemCache(new DirectoryInfo(""));
        }


        public async Task AppendValues(TimeValue<T>[] values)
        {
            foreach (var shard in _sharding.ShardValues(values))
            {
                var group = shard.GetValues();
                using (var stream = CreateMemoryStream())
                {
                    using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
                        foreach (var value in group)
                            _builder.Serialize(value, writer);

                    int writtenBytes = group.Count() * _builder.SingleElementSizeInBytes();

                    var blob = await OpenBlobAsync(shard.GetName());
                    stream.Seek(0, SeekOrigin.Begin);
                    await blob.AppendAsync(stream);
                }
            }
        }


        public IEnumerable<TimeValue<T>> GetValues(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(_directory.ListBlobs(), from, to))
            {
                var shardvalues = FetchShard(shard.GetName()).OrderBy(d => d.Timestamp);

                foreach (var v in shardvalues)
                    if (v.Timestamp.Between(from, to))
                        yield return v;
            }
        }

        public IEnumerable<LazyTimeValue<T>> GetLazyValues(DateTime? from, DateTime? to)
        {
            throw new NotImplementedException();
        }

        IEnumerable<TimeValue<T>> FetchShard(string name)
        {
            var task = OpenBlobAsync(name);
            task.Wait();

            var a = task.Result.DownloadValuesAsync();
            a.Wait();

            var stream = a.Result;
            using (var reader = new BinaryReader(stream))
                while (stream.Position != stream.Length)
                    yield return _builder.Deserialize(reader);
        }



        public IEnumerable<byte[]> GetRawValues(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(_directory.ListBlobs(), from, to))
            {
                var task = OpenBlobAsync(shard.GetName());
                task.Wait();

                var a = task.Result.DownloadValuesAsync();
                a.Wait();

                var stream = a.Result;
                using (var reader = new BinaryReader(stream))
                    while (stream.Position != stream.Length)
                    {
                        var data = new byte[_builder.SingleElementSizeInBytes()];
                        stream.Read(data, 0, data.Length);
                        yield return data;
                    }
            }
        }



        public void HintSamplingPeriod(int samplingPeriodInSeconds)
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


        Stream CreateMemoryStream()
        {
            //return new BufferPoolStream(new BufferPool());
            return new MemoryStream();
        }

        public void Reset()
        {
            foreach (var b in _directory.ListBlobs())
                //if (b.Uri.ToString().Contains(AzureBlobFactory.Sharding) == false)
                b.Container.GetPageBlobReference(b.Uri.ToString()).Delete();
        }
    }

}
