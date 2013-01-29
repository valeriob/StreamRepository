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
        IEnumerable<GroupOfValues> GroupBy(IEnumerable<Tuple<DateTime, double, int>> values);

        IEnumerable<Group> GetGroups(DateTime? from = null, DateTime? to = null);
    }

    public interface Group
    {
        string GetName();
    }

    public interface GroupOfValues : Group
    {
        IEnumerable<Tuple<DateTime, double, int>> GetValues();
    }



    public class NoShardingStrategy : ShardingStrategy
    {
        public IEnumerable<GroupOfValues> GroupBy(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            yield return new NoGroup(values);
        }
        public IEnumerable<Group> GetGroups(DateTime? from = null, DateTime? to = null)
        {
            throw new NotImplementedException();
        }

        public class NoGroup : GroupOfValues
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

    public class FilePerYearShardingStrategy : ShardingStrategy
    {
        DirectoryInfo _folder;

        public FilePerYearShardingStrategy(DirectoryInfo folder)
        {
            _folder = folder;
        }


        public IEnumerable<GroupOfValues> GroupBy(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            return values.GroupBy(g => g.Item1.Year).Select(g=> new YearGroup(g.Key, g));
        }

        public IEnumerable<Group> GetGroups(DateTime? from = null, DateTime? to = null)
        {
            return _folder.GetFiles().Select(f => new
            {
                f,
                f.Name,
                Year = Convert.ToInt32(Path.GetFileNameWithoutExtension(f.FullName))
            }).OrderBy(d => d.Year)
           .Select(s => new YearGroup(s.Year, Enumerable.Empty<Tuple<DateTime, double, int>>()));
        }


        public class YearGroup : GroupOfValues
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

    public class FilePerMonthShardingStrategy : ShardingStrategy
    {
        public IEnumerable<GroupOfValues> GroupBy(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            return values.GroupBy(g => new { g.Item1.Year, g.Item1.Month }).Select(g => new MonthGroup(g.Key.Year, g.Key.Month, g));
        }

        public IEnumerable<Group> GetGroups(DateTime? from = null, DateTime? to = null)
        {
            throw new NotImplementedException();
        }

        public class MonthGroup : GroupOfValues
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
