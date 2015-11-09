﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;


namespace StreamRepository.FileSystem
{
    public interface FileSystemShardingStrategy<T>
    {
        IEnumerable<ShardWithValues<T>> ShardValues(TimeValue<T>[] values);

        IEnumerable<FileInfo> FilterFiles(IEnumerable<FileInfo> files, DateTime? from = null, DateTime? to = null);

        string GetId();
    }


    public class FileSystemPerYearShardingStrategy<T> : FileSystemShardingStrategy<T>
    {

        public IEnumerable<ShardWithValues<T>> ShardValues(TimeValue<T>[] values)
        {
            return values.GroupBy(g => g.Timestamp.Year).Select(g => new YearGroup(g.Key, g.ToArray()));
        }

        public IEnumerable<FileInfo> FilterFiles(IEnumerable<FileInfo> files, DateTime? from = null, DateTime? to = null)
        {
            foreach (var file in files.Where(f=> !f.Name.StartsWith(Consts.Sharding)))
            {
                int year = int.Parse(file.Name);

                if (Shard_Is_In_Between(from, to, year))
                    yield return file;
            }
        }

        bool Shard_Is_In_Between(DateTime? from, DateTime? to, int year)
        {
            if (from == null && to == null)
                return true;
            return (from == null || from.Value.Year < year) && (to == null || to.Value.Year > year);
        }

        public string GetId()
        {
            return Guid.Parse("9C2880C1-16D7-4D90-8D37-CC3D7231EAB0").ToString();
        }
        public class YearGroup : ShardWithValues<T>
        {
            int _year;
            TimeValue<T>[] _values;

            public YearGroup(int year, TimeValue<T>[] values)
            {
                _year = year;
                _values = values;
            }

            public TimeValue<T>[] GetValues()
            {
                return _values;
            }

            public string GetName()
            {
                return _year + "";
            }
        }
    }

    public class FileSystemPerMonthShardingStrategy<T> : FileSystemShardingStrategy<T>
    {
        public IEnumerable<ShardWithValues<T>> ShardValues(TimeValue<T>[] values)
        {
            return values.GroupBy(g => new { g.Timestamp.Year, g.Timestamp.Month }).Select(g => new MonthGroup(g.Key.Year, g.Key.Month, g.ToArray()));
        }

        public IEnumerable<FileInfo> FilterFiles(IEnumerable<FileInfo> files, DateTime? from = null, DateTime? to = null)
        {
            foreach (var file in files)
            {
                if (file.Name.StartsWith("sharding-"))
                    continue;
                var tokens = file.Name.Split('-');

                int year = int.Parse(tokens[0]);
                int month = int.Parse(tokens[1]);

                if (Shard_Is_In_Between(from, to, year, month))
                    yield return file;
                    //yield return new MonthGroup(year, month, null);
            }
        }

        bool Shard_Is_In_Between(DateTime? from, DateTime? to, int year, int month)
        {
            if (from == null && to == null)
                return true;

            var date = new DateTime(year, month, 1);
            return (from == null || from.Value < date) && (to == null || to.Value > date);
        }


        public string GetId()
        {
            return Guid.Parse("CAABA129-479F-4F36-B5B9-B08C59EEB6CF").ToString();
        }

        public class MonthGroup : ShardWithValues<T>
        {
            int _year;
            int _month;
            TimeValue<T>[] _values;

            public MonthGroup(int year, int month, TimeValue<T>[] values)
            {
                _year = year;
                _month = month;
                _values = values;
            }

            public TimeValue<T>[] GetValues()
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
