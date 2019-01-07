using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileSystemRepository<T> : Repository<T> where T : ITimeValue
    {
        DirectoryInfo _directory;
        FileSystemShardingStrategy<T> _sharding;
        ISerializeTimeValue<T> _builder;


        public FileSystemRepository(DirectoryInfo directory, FileSystemShardingStrategy<T> sharding, ISerializeTimeValue<T> builder)
        {
            _sharding = sharding;
            _directory = directory;
            _builder = builder;
        }


        public void AppendValues(T[] allValues)
        {
            foreach (var shardedValues in _sharding.ShardValues(allValues))
            {
                var shard = _sharding.GetShard(_directory, shardedValues.GetName());
                shard.AppendValues(_builder, shardedValues.GetValues());
            }
        }

        public async Task AppendValuesAsync(T[] allValues)
        {
            foreach (var shardedValues in _sharding.ShardValues(allValues))
            {
                var shard = _sharding.GetShard(_directory, shardedValues.GetName());
                await shard.AppendValuesAsync(_builder, shardedValues.GetValues());
            }
        }


        public IEnumerable<T> GetValues(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.SortShards(_directory, from, to))
            {
                //single
                //var values = shard.FetchValues(_builder);
                //foreach (var value in values)
                //{
                //    if (value.Timestamp.Between(from, to))
                //        yield return value;
                //}
                //batch
                var batches = shard.FetchValuesBatch(_builder);
                foreach (var batch in batches)
                {
                    foreach (var value in batch)
                    {
                        if (value.Timestamp.Between(from, to))
                            yield return value;
                    }
                }
            }
        }

        public void Compact()
        {
            _sharding.Compact(_directory, _builder);
        }

        public Task CompactAsync()
        {
            return _sharding.CompactAsync(_directory, _builder);
        }

    }


}
