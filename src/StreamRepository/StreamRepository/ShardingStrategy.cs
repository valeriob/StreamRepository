using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public interface ShardingStrategy
    {
        IEnumerable<ShardWithValues> Shard(IEnumerable<Tuple<DateTime, double, int>> values);

        IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null);
    }

    public interface Shard
    {
        string GetName();
    }

    public interface ShardWithValues : Shard
    {
        IEnumerable<Tuple<DateTime, double, int>> GetValues();
    }



    public class NoShardingStrategy : ShardingStrategy
    {
        public IEnumerable<ShardWithValues> Shard(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            yield return new NoGroup(values);
        }
        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            throw new NotImplementedException();
        }

        public class NoGroup : ShardWithValues
        {
            IEnumerable<Tuple<DateTime, double, int>> _values;

            public NoGroup(IEnumerable<Tuple<DateTime, double, int>> values)
            {
                _values = values;
            }

            public IEnumerable<Tuple<DateTime, double, int>> GetValues()
            {
                return _values;
            }

            public string GetName()
            {
                return "data";
            }
        }
    }

}
