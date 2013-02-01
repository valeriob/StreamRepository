﻿using EventStore.BufferManagement;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.Azure
{
    public class AzureBlobRepository : Repository
    {
        CloudBlobDirectory _directory;
        Func<int, string> _logFileName = year => string.Format("{0}", year);
        Dictionary<string, PageBlobState> _cache;
        ShardingStrategy _sharding;


        public AzureBlobRepository(CloudBlobDirectory directory, ShardingStrategy sharding)
        {
            _directory = directory;
            _sharding = sharding;
            _cache = new Dictionary<string, PageBlobState>();
        }

 
        public override void Append_Values(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            foreach (var shard in _sharding.Shard(values))
            {
                var group = shard.GetValues();
                using (var stream = new MemoryStream())
                {
                    using (var writer = new BinaryWriter(stream))
                        foreach (var value in group)
                            new FramedValue(value.Item1, value.Item2, value.Item3).Serialize(writer);

                    int writtenBytes = group.Count() * FramedValue.SizeInBytes();

                    var blob = OpenBlobFor(shard.GetName());

                    blob.Append(stream.GetBuffer(), 0, writtenBytes);
                }
            }
        }

        public override IEnumerable<RecordValue> Get_Values(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(from, to))
            {
                var blob = OpenBlobFor(shard.GetName());
                foreach (var value in blob.Read_Values())
                    yield return value;
            }
        }

        public override  IEnumerable<byte[]> Get_Raw_Values(DateTime? from, DateTime? to)
        {
            foreach (var shard in _sharding.GetShards(from, to))
            {
                var blob = OpenBlobFor(shard.GetName());
                foreach (var value in blob.Read_Raw_Values())
                    yield return value;
            }
        }

        IEnumerable<int> Get_Years()
        {
            foreach (var blob in _directory.ListBlobs())
            {
                var last = blob.Uri.Segments.Last();
                int year;
                if (Int32.TryParse(last, out year))
                    yield return year;
            }
        }

        public override void Hint_Sampling_Period(int samplingPeriodInSeconds)
        {
            // TODO
           // OpenBlobFor(year).Ensure_There_Is_Space_For(samples * FramedValue.SizeInBytes());
        }

        PageBlobState OpenBlobFor(string name)
        {
            PageBlobState blob;
            if (!_cache.TryGetValue(name, out blob))
            {
                blob = new PageBlobState(_directory, name);
                Create_And_Register_Blob_On_Index_If_Necessary(blob, name);
                blob.Open();
                _cache[name] = blob;
            }
            return blob;
        }

        void Create_And_Register_Blob_On_Index_If_Necessary(PageBlobState blob, string name)
        {
            if (!blob.Exists())
            {
                blob.Create();

                var index = new PageBlobState(_directory, NamingUtilities.Get_Index_File(_directory));
                index.Open();
                index.Append(name + Environment.NewLine);
            }
        }

    }

}
