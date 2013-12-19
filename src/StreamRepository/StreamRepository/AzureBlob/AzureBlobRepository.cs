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
        IBuildStuff _builder;


        public AzureBlobRepository(CloudBlobDirectory directory, AzureBlobShardingStrategy sharding, IBuildStuff builder)
        {
            _directory = directory;
            _sharding = sharding;
            _builder = builder;
            _blobsCache = new Dictionary<string, AzurePageBlob>();
            //_cache = new FileSystemCache(new DirectoryInfo(""));
        }


        public async Task AppendValues(IEnumerable<ICanBeSharded> values)
        {
            foreach (var shard in _sharding.Shard(values))
            {
                var group = shard.GetValues();
                //using (var stream = new BufferPoolStream(new BufferPool()))
                using (var stream = new MemoryStream())
                {
                    using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
                        foreach (var value in group)
                            _builder.Serialize(value, writer);
                    //new FramedValue(value.Timestamp, value.Value, value.ImportId).Serialize(writer);

                    int writtenBytes = group.Count() * _builder.SizeInBytes();

                    var blob = await OpenBlobAsync(shard.GetName());
                    stream.Seek(0, SeekOrigin.Begin);
                    await blob.AppendAsync(stream);
                }
            }
        }

        public IEnumerable<object> GetValues(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(_directory.ListBlobs(), from, to))
            {
                var name = shard.GetName();

                var task = OpenBlobAsync(name);
                task.Wait();

                var a = task.Result.DownloadValuesAsync();
                a.Wait();

                var stream = a.Result;
                using (var reader = new BinaryReader(stream))
                    while (stream.Position != stream.Length)
                    {
                        yield return _builder.Deserialize(reader);
                        //object value = null;
                        //try
                        //{
                        //    _builder.Deserialize(reader);
                        //}
                        //catch (EndOfStreamException)
                        //{
                        //    break;
                        //}
                        //yield return value;
                    }
            }
        }

        public IEnumerable<byte[]> Get_Raw_Values(DateTime? from, DateTime? to)
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
                        var data = new byte[_builder.SizeInBytes()];
                        stream.Read(data, 0, data.Length);
                        yield return data;
                    }
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



        public void Reset()
        {
            foreach (var b in _directory.ListBlobs())
                //if (b.Uri.ToString().Contains(AzureBlobFactory.Sharding) == false)
                b.Container.GetPageBlobReference(b.Uri.ToString()).Delete();
        }
    }

}
