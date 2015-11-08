using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public interface ShardingStrategy<T> where T : TimeValue<T>
    {
        IEnumerable<ShardWithValues<T>> Shard(IEnumerable<TimeValue<T>> values);

        IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null);
    }

    public interface Shard
    {
        string GetName();
    }

    public interface ShardWithValues<T> : Shard
    {
        TimeValue<T>[] GetValues();
    }


    [Export(typeof(ShardingStrategy<>))]
    [Guid("8308DEEC-DDDB-4719-B6C7-DF9233E3AFBB")]
    public class NoShardingStrategy<T> : ShardingStrategy<T> where T : TimeValue<T>
    {
        public IEnumerable<ShardWithValues<T>> Shard(IEnumerable<TimeValue<T>> values)
        {
            yield return new NoGroup(values.ToArray());
        }
        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            throw new NotImplementedException();
        }

        public class NoGroup : ShardWithValues<T>
        {
            TimeValue<T>[] _values;

            public NoGroup(TimeValue<T>[] values)
            {
                _values = values;
            }

            public TimeValue<T>[] GetValues()
            {
                return _values;
            }

            public string GetName()
            {
                return "data";
            }
        }

        public Guid GetId()
        {
            return Guid.Parse(GetType().GetAttribute<GuidAttribute>().Value);
        }
    }

}
