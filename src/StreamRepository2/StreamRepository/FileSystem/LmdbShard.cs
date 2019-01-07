using LightningDB;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class LmdbShard<T> : IShard<T> where T : ITimeValue
    {
        protected object _syncRoot;
        protected string _shardName;
        protected DirectoryInfo _dir;


        public LmdbShard(string shardName, DirectoryInfo dir)
        {
            _shardName = shardName;
            _dir = dir;
            _syncRoot = new object();

        }


        public void AppendValues(ISerializeTimeValue<T> builder, IEnumerable<T> shardValues)
        {
            using (var env = OpenEnv())
            using (var tx = env.BeginTransaction())
            using (var db = tx.OpenDatabase(null, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create  }))
            {
                var keys = new HashSet<byte[]>();

                foreach (var value in shardValues)
                {
                    //var key = Encoding.UTF8.GetBytes(value.Id);
                    //var data = builder.Serialize(value);
                    //tx.Put(db, key, data);
                    var key = BitConverter.GetBytes(value.Timestamp.ToBinary());
                    key = new byte[] { 1 };
                    var data = builder.Serialize(value);
                    data = new byte[] { 2 };
                    if (keys.Contains(key))
                    {
                        Debugger.Break();
                    }
                    keys.Add(key);

                    tx.Put(db, key, data, PutOptions.ReserveSpace);
                }
                tx.Commit();
            }

            using (var env = OpenEnv())
            using (var tx = env.BeginTransaction())
            using (var db = tx.OpenDatabase(null, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create }))
            {
                var keys = new List<KeyValuePair<byte, byte>>();
                using (var c = tx.CreateCursor(db))
                {
                    foreach (var kv in c)
                    {
                        keys.Add(new KeyValuePair<byte, byte>(kv.Key[0], kv.Value[0]));

                    }
                }
            }
        }

        LightningEnvironment OpenEnv()
        {
            var p = Path.Combine(_dir.FullName, _shardName);
            Directory.CreateDirectory(p);
            var env = new LightningEnvironment(p, new EnvironmentConfiguration
            {
                MaxDatabases = 1,
                MapSize = 1024 * 1024 * 1024,

            });
            env.Open();
            return env;
        }

        public Task AppendValuesAsync(ISerializeTimeValue<T> builder, IEnumerable<T> shardValues)
        {
            throw new NotImplementedException();
        }

        public void Compact(ISerializeTimeValue<T> builder)
        {

        }

        public IEnumerable<T> FetchValues(ISerializeTimeValue<T> builder)
        {
            using (var env = OpenEnv())
            using (var tx = env.BeginTransaction())
            using (var db = tx.OpenDatabase(null, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create }))
            using (var cursor = tx.CreateCursor(db))
            {
                foreach (var item in cursor)
                {
                    yield return builder.Deserialize(item.Value);
                }
            }

        }


        public IEnumerable<T[]> FetchValuesBatch(ISerializeTimeValue<T> builder)
        {
            throw new NotImplementedException();
        }

    }
}
