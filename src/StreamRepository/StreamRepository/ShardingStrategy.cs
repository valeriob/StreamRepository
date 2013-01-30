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
    public interface ShardingStrategyFactory
    {
        ShardingStrategy Create(Guid strategyId);
    }

    public interface ShardingStrategy
    {
        Guid GetId();

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


    public class ShardingStrategyFactoryImpl : ShardingStrategyFactory
    {
        [ImportMany(typeof(ShardingStrategy))]
        static List<ShardingStrategy> _strategies;

        static CompositionContainer _container;

        static ShardingStrategyFactoryImpl()
        {
            var _catalog = new AggregateCatalog();
            _catalog.Catalogs.Add(new AssemblyCatalog(typeof(ShardingStrategyFactoryImpl).Assembly));

            _container = new CompositionContainer(_catalog, CompositionOptions.DisableSilentRejection | CompositionOptions.IsThreadSafe);
        }

        static ShardingStrategyFactory _instance;
        public static ShardingStrategyFactory Instance()
        {
            if (_instance == null)
                _instance = new ShardingStrategyFactoryImpl();

            return _instance;
        }


        Dictionary<string, ShardingStrategy> _dictionary;
        ShardingStrategyFactoryImpl()
        {
            _container.SatisfyImportsOnce(this);
            _dictionary = _strategies.ToDictionary(s => s.GetType().GetAttribute<GuidAttribute>().Value);
        }

        public ShardingStrategy Create(Guid strategyId)
        {
            ShardingStrategy strategy;
            if (!_dictionary.TryGetValue(strategyId + "", out strategy))
                throw new Exception("strategy could not be found : " + strategyId);

            return strategy;
        }

        

    }

    [Export(typeof(ShardingStrategy))]
    [Guid("8308DEEC-DDDB-4719-B6C7-DF9233E3AFBB")]
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

        public Guid GetId()
        {
            return Guid.Parse(GetType().GetAttribute<GuidAttribute>().Value);
        }
    }

}
