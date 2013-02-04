using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    [Export(typeof(ShardingStrategy))]
    [Guid("9C2880C1-16D7-4D90-8D37-CC3D7231EAB0")]
    public class FileSystemPerYearShardingStrategy : ShardingStrategy
    {
        IEnumerable<FileInfo> _files;

        public FileSystemPerYearShardingStrategy(IEnumerable<FileInfo> files)
        {
            _files = files;
        }


        public IEnumerable<ShardWithValues> Shard(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            return values.GroupBy(g => g.Item1.Year).Select(g => new YearGroup(g.Key, g));
        }

        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            foreach (var file in _files)
            {
                int year = int.Parse(file.Name);

                if (Shard_Is_In_Between(from, to, year))
                    yield return new YearGroup(year, null);
            }
           // return _directory.GetFiles().Select(f => new
           // {
           //     f,
           //     f.Name,
           //     Year = Convert.ToInt32(Path.GetFileNameWithoutExtension(f.FullName))
           // }).OrderBy(d => d.Year)
           //.Select(s => new YearGroup(s.Year, Enumerable.Empty<Tuple<DateTime, double, int>>()));
        }

        public Guid GetId()
        {
            return Guid.Parse(GetType().GetAttribute<GuidAttribute>().Value);
        }
        bool Shard_Is_In_Between(DateTime? from, DateTime? to, int year)
        {
            if (from == null && to == null)
                return true;
            return (from == null || from.Value.Year < year) && (to == null || to.Value.Year > year);
        }

        public class YearGroup : ShardWithValues
        {
            int _year;
            IEnumerable<Tuple<DateTime, double, int>> _values;

            public YearGroup(int year, IEnumerable<Tuple<DateTime, double, int>> values)
            {
                _year = year;
                _values = values;
            }

            public IEnumerable<Tuple<DateTime, double, int>> GetValues()
            {
                return _values;
            }

            public string GetName()
            {
                return _year + "";
            }
        }
    }

    [Export(typeof(ShardingStrategy))]
    [Guid("CAABA129-479F-4F36-B5B9-B08C59EEB6CF")]
    public class FileSystemPerMonthShardingStrategy : ShardingStrategy
    {
        IEnumerable<FileInfo> _files;

        public FileSystemPerMonthShardingStrategy(IEnumerable<FileInfo> files)
        {
            _files = files;
        }


        public IEnumerable<ShardWithValues> Shard(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            return values.GroupBy(g => new { g.Item1.Year, g.Item1.Month }).Select(g => new MonthGroup(g.Key.Year, g.Key.Month, g));
        }

        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            foreach (var file in _files)
            {
                var tokens = file.Name.Split('-');

                int year = int.Parse(tokens[0]);
                int month = int.Parse(tokens[1]);

                if (Shard_Is_In_Between(from, to, year, month))
                    yield return new MonthGroup(year, month, null);
            }
        }

        bool Shard_Is_In_Between(DateTime? from, DateTime? to, int year, int month)
        {
            if (from == null && to == null)
                return true;

            var date = new DateTime(year, month, 1);
            return (from == null || from.Value < date) && (to == null || to.Value > date);
        }

        public Guid GetId()
        {
            return Guid.Parse(GetType().GetAttribute<GuidAttribute>().Value);
        }

        public class MonthGroup : ShardWithValues
        {
            int _year;
            int _month;
            IEnumerable<Tuple<DateTime, double, int>> _values;

            public MonthGroup(int year, int month, IEnumerable<Tuple<DateTime, double, int>> values)
            {
                _year = year;
                _month = month;
                _values = values;
            }

            public IEnumerable<Tuple<DateTime, double, int>> GetValues()
            {
                return _values;
            }

            public string GetName()
            {
                return string.Format("{0}-{1}", _year, _month);
            }
        }
    }

}
