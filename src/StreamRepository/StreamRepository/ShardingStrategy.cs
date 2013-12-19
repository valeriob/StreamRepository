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
    public interface ShardingStrategy
    {
        IEnumerable<ShardWithValues> Shard(IEnumerable<ICanBeSharded> values);

        IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null);
    }

    public interface Shard
    {
        string GetName();
    }

    public interface ShardWithValues : Shard
    {
        IEnumerable<ICanBeSharded> GetValues();
    }


    [Export(typeof(ShardingStrategy))]
    [Guid("8308DEEC-DDDB-4719-B6C7-DF9233E3AFBB")]
    public class NoShardingStrategy : ShardingStrategy
    {
        public IEnumerable<ShardWithValues> Shard(IEnumerable<ICanBeSharded> values)
        {
            yield return new NoGroup(values);
        }
        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            throw new NotImplementedException();
        }

        public class NoGroup : ShardWithValues
        {
            IEnumerable<ICanBeSharded> _values;

            public NoGroup(IEnumerable<ICanBeSharded> values)
            {
                _values = values;
            }

            public IEnumerable<ICanBeSharded> GetValues()
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
