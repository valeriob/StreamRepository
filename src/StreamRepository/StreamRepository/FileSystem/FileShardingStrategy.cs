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
    public class FilePerYearShardingStrategy : ShardingStrategy
    {
        DirectoryInfo _directory;

        public FilePerYearShardingStrategy(DirectoryInfo directory)
        {
            _directory = directory;
        }


        public IEnumerable<ShardWithValues> Shard(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            return values.GroupBy(g => g.Item1.Year).Select(g => new YearGroup(g.Key, g));
        }

        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            var index = FileUtilities.Get_Index_File(_directory);
            foreach (var file in File.ReadAllLines(index).Skip(1))
            {
                int year = int.Parse(file);

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
    public class FilePerMonthShardingStrategy : ShardingStrategy
    {
        DirectoryInfo _directory;

        public FilePerMonthShardingStrategy(DirectoryInfo directory)
        {
            _directory = directory;
        }


        public IEnumerable<ShardWithValues> Shard(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            return values.GroupBy(g => new { g.Item1.Year, g.Item1.Month }).Select(g => new MonthGroup(g.Key.Year, g.Key.Month, g));
        }

        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            var index = FileUtilities.Get_Index_File(_directory);
            foreach (var file in File.ReadAllLines(index).Skip(1))
            {
                var tokens = file.Split('-');

                int year = int.Parse(tokens[0]);
                int month = int.Parse(tokens[1]);

                // if(from != null && from.Value.Year < year &&
                yield return new MonthGroup(year, month, null);
            }
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
