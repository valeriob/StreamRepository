using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public abstract class FileSystemShardingStrategy<T> where T : ITimeValue
    {
        string _name;
        public FileSystemShardingStrategy(string name)
        {
            _name = name;
        }

        public abstract IEnumerable<ShardWithValues<T>> ShardValues(T[] values);

        public abstract IEnumerable<IShard<T>> SortShards(DirectoryInfo dir, DateTime? from = null, DateTime? to = null);

        public string GetName()
        {
            return _name;
        }


        public void Compact(DirectoryInfo dir, ISerializeTimeValue<T> builder)
        {
            var shards = SortShards(dir);
            foreach (var shard in shards)
            {
                shard.Compact(builder);
            }
        }

        public async Task CompactAsync(DirectoryInfo dir, ISerializeTimeValue<T> builder)
        {
            var shards = SortShards(dir);
            foreach (var shard in shards)
            {
                while (true)
                {
                    try
                    {
                        //Console.WriteLine($"{DateTime.Now.TimeOfDay} Attempting to compact Shard {_name}");
                        shard.Compact(builder);
                       // Console.WriteLine($"{DateTime.Now.TimeOfDay} Compacted Shard {_name}");
                        break;
                    }
                    catch (FileLockException)
                    {
                        await Task.Delay(20);
                    }
                }
            }
        }

        public IShard<T> GetShard(DirectoryInfo dir, string name)
        {
            return new GZipFileShard<T>(name, dir);
            return new LmdbShard<T>(name, dir);
        }

        protected FileInfo[] GetShardDataFiles(DirectoryInfo dir)
        {
            var result = dir.GetFiles().Where(f => (f.Name.StartsWith(Consts.Sharding) == false) && (Path.GetExtension(f.Name) != Consts.Header) /*&& (Path.GetExtension(f.Name) != "zip")*/).ToArray();
            return result;
        }
    }


    public class FileSystemPerYearShardingStrategy<T> : FileSystemShardingStrategy<T> where T : ITimeValue
    {
        public FileSystemPerYearShardingStrategy() : base("Year")
        {

        }

        public override IEnumerable<ShardWithValues<T>> ShardValues(T[] values)
        {
            return values.GroupBy(g => g.Timestamp.Year).Select(g => new YearGroup(g.Key, g.AsEnumerable()));
        }


        public override IEnumerable<IShard<T>> SortShards(DirectoryInfo dir, DateTime? from = null, DateTime? to = null)
        {
            var shards = GetShardDataFiles(dir).GroupBy(f => int.Parse(Path.GetFileNameWithoutExtension(f.Name))).OrderBy(g => g.Key);
            foreach (var shard in shards)
            {
                int year = shard.Key;
                if (Shard_Is_In_Between(from, to, year))
                    yield return new GZipFileShard<T>(shard.Key.ToString(), dir);
            }
        }



        bool Shard_Is_In_Between(DateTime? from, DateTime? to, int year)
        {
            if (from == null && to == null)
                return true;
            return (from == null || from.Value.Year < year) && (to == null || to.Value.Year > year);
        }


        public class YearGroup : ShardWithValues<T>
        {
            int _year;
            IEnumerable<T> _values;

            public YearGroup(int year, IEnumerable<T> values)
            {
                _year = year;
                _values = values;
            }

            public IEnumerable<T> GetValues()
            {
                return _values;
            }

            public string GetName()
            {
                return _year + "";
            }
        }
    }

    public class FileSystemPerMonthShardingStrategy<T> : FileSystemShardingStrategy<T> where T : ITimeValue
    {
        public FileSystemPerMonthShardingStrategy() : base("Month")
        {

        }
        public override IEnumerable<ShardWithValues<T>> ShardValues(T[] values)
        {
            return values.GroupBy(g => new { g.Timestamp.Year, g.Timestamp.Month }).Select(g => new MonthGroup(g.Key.Year, g.Key.Month, g.ToArray()));
        }

        public override IEnumerable<IShard<T>> SortShards(DirectoryInfo dir, DateTime? from = null, DateTime? to = null)
        {
            var shards = GetShardDataFiles(dir).GroupBy(f =>
            {
                var tokens = Path.GetFileNameWithoutExtension(f.Name).Split('-');
                int year = int.Parse(tokens[0]);
                int month = int.Parse(tokens[1]);
                return new DateTime(year, month, 1);
            }).OrderBy(g => g.Key);
            foreach (var shard in shards)
            {
                var date = shard.Key;
                if (Shard_Is_In_Between(from, to, date.Year, date.Month))
                    yield return new GZipFileShard<T>(shard.Key.ToString("yyyy-MM"), dir);
            }
        }


        bool Shard_Is_In_Between(DateTime? from, DateTime? to, int year, int month)
        {
            if (from == null && to == null)
                return true;

            var date = new DateTime(year, month, 1);
            return (from == null || from.Value < date) && (to == null || to.Value > date);
        }




        public class MonthGroup : ShardWithValues<T>
        {
            int _year;
            int _month;
            IEnumerable<T> _values;

            public MonthGroup(int year, int month, IEnumerable<T> values)
            {
                _year = year;
                _month = month;
                _values = values;
            }

            public IEnumerable<T> GetValues()
            {
                return _values;
            }

            public string GetName()
            {
                return string.Format("{0}-{1}", _year, _month);
            }
        }
    }

    public class FileSystemPerDayShardingStrategy<T> : FileSystemShardingStrategy<T> where T : ITimeValue
    {
        public FileSystemPerDayShardingStrategy() : base("Day")
        {

        }

        public override IEnumerable<ShardWithValues<T>> ShardValues(T[] values)
        {
            return values.GroupBy(g => g.Timestamp.Date).Select(g => new DayGroup(g.Key.Year, g.Key.Month, g.Key.Day, g.ToArray()));
        }


        public override IEnumerable<IShard<T>> SortShards(DirectoryInfo dir, DateTime? from = null, DateTime? to = null)
        {
            var shards = GetShardDataFiles(dir).GroupBy(f =>
            {
                var tokens = Path.GetFileNameWithoutExtension(f.Name).Split('-');
                int year = int.Parse(tokens[0]);
                int month = int.Parse(tokens[1]);
                int day = int.Parse(tokens[2]);
                return new DateTime(year, month, day);
            }).OrderBy(g => g.Key);
            foreach (var shard in shards)
            {
                var date = shard.Key;
                if (Shard_Is_In_Between(from, to, date))
                    yield return new GZipFileShard<T>(shard.Key.ToString("yyyy-MM-dd"), dir);
                    //yield return new LmdbShard<T>(shard.Key.ToString("yyyy-MM-dd"), dir);
            }
        }


        bool Shard_Is_In_Between(DateTime? from, DateTime? to, DateTime date)
        {
            if (from == null && to == null)
                return true;
            return (from == null || from.Value < date) && (to == null || to.Value > date);
        }


        public class DayGroup : ShardWithValues<T>
        {
            int _year;
            int _month;
            int _day;
            IEnumerable<T> _values;

            public DayGroup(int year, int month, int day, IEnumerable<T> values)
            {
                _year = year;
                _month = month;
                _day = day;
                _values = values;
            }

            public IEnumerable<T> GetValues()
            {
                return _values;
            }

            public string GetName()
            {
                return string.Format("{0}-{1:00}-{2:00}", _year, _month, _day);
            }
        }
    }
}
