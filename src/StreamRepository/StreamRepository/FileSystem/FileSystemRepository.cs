﻿using EventStore.BufferManagement;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileSystemRepository<T> : Repository<T>
    {
        Dictionary<string, FileInfo> _fileCache;
        Func<int, string> _logFileName = year => string.Format("{0}.dat", year);
        Func<int, string> _logObsoleteFileName = year => string.Format("{0}.dat", year);
        DirectoryInfo _directory;
        FileSystemShardingStrategy<T> _sharding;
        ISerializeTimeValue<T> _builder;


        public FileSystemRepository(DirectoryInfo folder, FileSystemShardingStrategy<T> sharding, ISerializeTimeValue<T> builder)
        {
            _sharding = sharding;
            _directory = folder;
            _builder = builder;
            _fileCache = new Dictionary<string, FileInfo>();
        }


        public async Task AppendValues(TimeValue<T>[] values)
        {
            foreach (var shard in _sharding.ShardValues(values))
            {
                var group = shard.GetValues();
                var name = shard.GetName();
                var header = ReadHeader(name);
                DateTime lastEventTimestamp = header.LastEventTimestamp;
                Nullable<TimeValue<T>> lastEvent = null;

                using (var stream = Open_Stream_For_Writing(name))
                {
                    var tail = header.Index;

                    using (var buffer = CreateMemoryStream())
                    {
                        using (var writer = new BinaryWriter(buffer, Encoding.Default, true))
                        {
                            for (int i = 0; i < group.Length; i++)
                            {
                                var value = group[i];
                                _builder.Serialize(value, writer);
                                if (value.Timestamp > lastEventTimestamp)
                                {
                                    lastEvent = value;
                                    lastEventTimestamp = value.Timestamp;
                                }
                            }
                        }
                        stream.Seek(tail, SeekOrigin.Begin);
                        buffer.Seek(0, SeekOrigin.Begin);
                        int writtenBytes = group.Count() * _builder.SingleElementSizeInBytes();
                        await buffer.CopyToAsync(stream);
                        stream.Flush();

                        tail = tail + writtenBytes;
                    }

                    header.Update(lastEvent?.Timestamp, tail);
                }
                await WriteHeader(header, name);
            }
        }

        public IEnumerable<TimeValue<T>> GetValues(DateTime? from, DateTime? to)
        {
            foreach (var files in _sharding.FilterFiles(_directory.GetFiles(), from, to))
            {
                var shardvalues = FetchShard(files.FullName).OrderBy(d => d.Timestamp);

                foreach (var v in shardvalues)
                    if (v.Timestamp.Between(from, to))
                        yield return v;
            }
        }
        public IEnumerable<LazyTimeValue<T>> GetLazyValues(DateTime? from, DateTime? to)
        {
            foreach (var files in _sharding.FilterFiles(_directory.GetFiles(), from, to))
            {
                var shardvalues = FetchShardLazy(files.FullName).OrderBy(d => d.Timestamp);

                foreach (var v in shardvalues)
                    if (v.Timestamp.Between(from, to))
                        yield return v;
            }
        }

        IEnumerable<TimeValue<T>> FetchShard(string shardName)
        {
            using (var file = Open_Stream_For_Reading(shardName))
            {
                var reader = new BinaryReader(file);
                var header = StreamHeader.Deserialize(reader);

                file.Seek(StreamHeader.SizeInBytes(), SeekOrigin.Begin);
                reader = new BinaryReader(file);
                //var data = new ICanBeSharded[spots];
                while (file.Position < header.Index)
                    yield return _builder.Deserialize(reader);
                //int spots = (int)(file.Length - StreamHeader.SizeInBytes()) / _builder.SingleElementSizeInBytes();
                //return (ICanBeSharded[])_builder.Deserialize2(reader, spots);
            }
        }

        IEnumerable<LazyTimeValue<T>> FetchShardLazy(string shardName)
        {
            var values = GetRawShard(shardName);
            foreach (var value in values)
            {
                yield return _builder.DeserializeLazy(value);
            }
        }


        public IEnumerable<byte[]> GetRawValues(DateTime? from = null, DateTime? to = null)
        {
            foreach (var files in _sharding.FilterFiles(_directory.GetFiles(), from, to))
            {
                var values = GetRawShard(files.FullName);
                foreach (var value in values)
                    yield return value;
            }
        }

        IEnumerable<byte[]> GetRawShard(string shardName)
        {
            using (var file = Open_Stream_For_Reading(shardName))
            {
                var reader = new BinaryReader(file);
                var header = StreamHeader.Deserialize(reader);

                file.Seek(StreamHeader.SizeInBytes(), SeekOrigin.Begin);
                while (file.Position < header.Index)
                {
                    var data = new byte[_builder.SingleElementSizeInBytes()];
                    file.Read(data, 0, data.Length);
                    yield return data;
                }
            }
        }

        StreamHeader ReadHeader(string name)
        {
            try
            {
                using (var stream = Open_Stream_For_Reading(name))
                {
                    var reader = new BinaryReader(stream);

                    return StreamHeader.Deserialize(reader);
                }
            }
            catch (EndOfStreamException)
            {
                return new StreamHeader() { Index = StreamHeader.SizeInBytes() };
            }
        }

        async Task WriteHeader(StreamHeader header, string name)
        {
            using (var buffer = CreateMemoryStream())
            {
                using (var writer = new BinaryWriter(buffer, Encoding.UTF8, true))
                {
                    header.Serialize(writer);
                    writer.Flush();
                }

                buffer.Seek(0, SeekOrigin.Begin);

                using (var stream = Open_Stream_For_Writing(name))
                {
                    await buffer.CopyToAsync(stream);
                    stream.Flush();
                }
            }
        }

        Stream Open_Stream_For_Reading(string name)
        {
            return Get_FileInfo_With_Caching(name).OpenRead();
        }

        Stream Open_Stream_For_Writing(string name)
        {
            return new FileStream(Get_FileInfo_With_Caching(name).FullName, FileMode.Open, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough | FileOptions.SequentialScan);
            // return new FileStream_With_Hard_Flush(Get_Year_With_Caching(year).FullName, FileMode.Open);
            return Get_FileInfo_With_Caching(name).Open(FileMode.Open);
        }



        FileInfo Get_FileInfo(string name)
        {
            foreach (var file in _directory.GetFiles())
                if (file.Name == name)
                    return file;

            var path = Path.Combine(_directory.FullName, name);

            using (var stream = File.Create(path))
            {
                var header = new StreamHeader
                {
                    Index = StreamHeader.SizeInBytes(),
                    Timestamp = DateTime.Now
                };

                using (var writer = new BinaryWriter(stream))
                    header.Serialize(writer);
            }
            return new FileInfo(path);
        }

        FileInfo Get_FileInfo_With_Caching(string name)
        {
            FileInfo file = null;
            if (!_fileCache.TryGetValue(name, out file))
            {
                file = Get_FileInfo(name);
                _fileCache[name] = file;
            }
            return file;
        }

        Stream CreateMemoryStream()
        {
            //return new BufferPoolStream(new BufferPool());
            return new MemoryStream();
        }


        public void HintSamplingPeriod(int samplingPeriodInSeconds)
        {
            // TODO
            //using (var file = Get_Year_With_Caching(year).OpenWrite())
            //{
            //    var size = FramedValue.SizeInBytes() * samples;
            //    file.SetLength(size);
            //}
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }


    }


}
