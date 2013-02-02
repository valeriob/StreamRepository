﻿using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.Azure
{
    [Export(typeof(ShardingStrategy))]
    [Guid("0F87CB4A-13D4-4991-98FA-58EA5B95DE73")]
    public class BlobPerYearShardingStrategy : ShardingStrategy
    {
        IEnumerable<IListBlobItem> _blobs;

        public BlobPerYearShardingStrategy(IEnumerable<IListBlobItem> blobs)
        {
            _blobs = blobs;
        }


        public IEnumerable<ShardWithValues> Shard(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            return values.GroupBy(g => g.Item1.Year).Select(g => new YearGroup(g.Key, g));
        }

        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            var blobs = _blobs.Select(s => s.Uri.Segments.Last()).Where(s => !s.StartsWith(BlobFactory.Sharding)).ToList();
            var shards = new List<YearGroup>();

            foreach (var blob in blobs)
            {
                int year = int.Parse(blob);
                if (Shard_Is_In_Between(from, to, year))
                    shards.Add(new YearGroup(year, null));
            }
            return shards.OrderBy(s => s.Year);
        }

        bool Shard_Is_In_Between(DateTime? from, DateTime? to, int year)
        {
            if (from == null && to == null)
                return true;
            return (from == null || from.Value.Year <= year) && (to == null || to.Value.Year >= year);
        }

        public Guid GetId()
        {
            return Guid.Parse(GetType().GetAttribute<GuidAttribute>().Value);
        }

        public class YearGroup : ShardWithValues
        {
            int _year;
            IEnumerable<Tuple<DateTime, double, int>> _values;
            public int Year { get { return _year; } }


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
    [Guid("1D267B88-B620-4584-8C17-46B2B648FB20")]
    public class BlobPerMonthShardingStrategy : ShardingStrategy
    {
        IEnumerable<IListBlobItem> _blobs;

        public BlobPerMonthShardingStrategy(IEnumerable<IListBlobItem> blobs)
        {
            _blobs = blobs;
        }


        public IEnumerable<ShardWithValues> Shard(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            return values.GroupBy(g => new { g.Item1.Year, g.Item1.Month }).Select(g => new MonthGroup(g.Key.Year, g.Key.Month, g));
        }

        public IEnumerable<Shard> GetShards(DateTime? from = null, DateTime? to = null)
        {
            foreach (var blob in _blobs.Select(s => s.Uri.Segments.Last()).Where(s => !s.StartsWith(BlobFactory.Sharding)))
            {
                var tokens = blob.Split('-');

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
