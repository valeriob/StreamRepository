using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileShard<T> : IShard<T> where T : ITimeValue
    {
        protected object _syncRoot;
        protected string _shardName;
        protected DirectoryInfo _dir;

        public FileShard(string shardName, DirectoryInfo dir)
        {
            _shardName = shardName;
            _dir = dir;
            _syncRoot = new object();
        }

        public async Task AppendValuesAsync(ISerializeTimeValue<T> builder, IEnumerable<T> shardValues)
        {
            while (true)
            {
                try
                {
                    //Console.WriteLine($"{DateTime.Now.TimeOfDay} Attempting to append data to Shard {_shardName}");
                    TryAppendValues(builder, shardValues);
                    //Console.WriteLine($"{DateTime.Now.TimeOfDay} Appended data to Shard {_shardName}");
                    break;
                }
                catch (FileLockException)
                {
                    await Task.Delay(20);
                }
            }
        }

        public void AppendValues(ISerializeTimeValue<T> builder, IEnumerable<T> shardValues)
        {
            int count = 0;
            while (true)
            {
                try
                {
                    //Console.WriteLine($"{DateTime.Now.TimeOfDay} Attempting to append data to Shard {_shardName}");
                    TryAppendValues(builder, shardValues);
                    //Console.WriteLine($"{DateTime.Now.TimeOfDay} Appended data to Shard {_shardName}");
                    break;
                }
                catch (FileLockException)
                {
                    count++;
                    Thread.Sleep(20);
                }
            }

           //Console.WriteLine($"Lock acquired in {count} tries");
        }

        void TryAppendValues(ISerializeTimeValue<T> builder, IEnumerable<T> shardValues)
        {
            using (var l = new FileLockV2(_dir, _shardName))
            {
                using (var stream = Open_Stream_For_Writing(Path.Combine(_dir.FullName, _shardName)))
                {
                    StreamHeader header = ReadHeader(_shardName, DateTime.MinValue);

                    long initialPosition = header.Index;
                    long lastPosition = initialPosition;
                    stream.Seek(initialPosition, SeekOrigin.Begin);
                    int numberOfValues = 0;

                    foreach (var value in shardValues)
                    {
                        header.UpdateTimestamps(value.Timestamp);

                        builder.Serialize(value, stream);

                        lastPosition = stream.Position;
                        numberOfValues++;
                    }

                    header.Update(lastPosition, numberOfValues);

                    WriteHeader(header, _shardName);
                }
            }

        }



        public virtual IEnumerable<T[]> FetchValuesBatch(ISerializeTimeValue<T> builder)
        {
            var current = Current();
            if (current != null)
            {
                yield return FetchValuesBatchLazy(current, builder);
            }
        }
        protected T[] FetchValuesBatchLazy(FileInfo fileInfo, ISerializeTimeValue<T> builder)
        {
            using (var file = Open_Stream_For_Reading(fileInfo.FullName))
            {
                var header = ReadHeader(_shardName, DateTime.MinValue);
                return builder.DeserializeAll(file, header.Index);
            }
        }


        public virtual IEnumerable<T> FetchValues(ISerializeTimeValue<T> builder)
        {
            var current = Current();
            if (current != null)
            {
                return FetchValuesLazy(current, builder);
            }
            else
            {
                return Array.Empty<T>();
            }
        }

        public virtual void Compact(ISerializeTimeValue<T> builder)
        {
        }

        protected IEnumerable<T> FetchValuesLazy(FileInfo fileInfo, ISerializeTimeValue<T> builder)
        {
            using (var file = Open_Stream_For_Reading(fileInfo.FullName))
            {
                var header = ReadHeader(_shardName, DateTime.MinValue);
                while (file.Position < header.Index)
                    yield return builder.Deserialize(file);
            }
        }



        protected FileInfo Current()
        {
            return _dir.GetFiles(_shardName).SingleOrDefault();
        }

        StreamHeader ReadHeader(string shardName, DateTime firstEventTimestamp)
        {
            var headerFile = Path.Combine(_dir.FullName, shardName + Consts.Header);
            if (File.Exists(headerFile))
            {
                try
                {
                    using (var stream = Open_Stream_For_Reading(headerFile))
                    {
                        var reader = new BinaryReader(stream);

                        return StreamHeader.Deserialize(reader);
                    }
                }
                catch (EndOfStreamException)
                {

                }
            }

            return new StreamHeader(firstEventTimestamp);
        }

        void WriteHeader(StreamHeader header, string shardName)
        {
            var headerFile = Path.Combine(_dir.FullName, shardName + ".header");
            using (var stream = Open_Stream_For_Writing(headerFile))
            using (var writer = new BinaryWriter(stream))
            using (var sw = new StreamWriter(stream))
            {
                header.Serialize(writer);
                writer.Flush();
                sw.WriteLine($"Index:{header.Index}, Timestamp:{header.Timestamp}, First:{header.FirstEventTimestamp}, Last:{header.LastEventTimestamp}");
                sw.Flush();
            }
        }

        protected FileStream Open_Stream_For_Reading(string name)
        {
            return new FileStream(name, FileMode.Open, FileAccess.Read, FileShare.None, 4096, FileOptions.WriteThrough | FileOptions.SequentialScan);
        }

        protected FileStream Open_Stream_For_Writing(string name)
        {
            return new FileStream(name, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.WriteThrough | FileOptions.SequentialScan);
        }

        public void Dispose()
        {
            
        }
    }

    public class GZipFileShard<T> : FileShard<T> where T : ITimeValue
    {
        public GZipFileShard(string shardName, DirectoryInfo dir) : base(shardName, dir)
        {

        }

        public override IEnumerable<T> FetchValues(ISerializeTimeValue<T> builder)
        {
            var zipped = Zipped();
            if (zipped != null)
            {
                var values = GZipFetchValuesLazy(zipped, builder);
                foreach (var value in values)
                {
                    yield return value;
                }
            }

            var current = Current();
            if (current != null)
            {
                var values = FetchValuesLazy(current, builder);
                foreach (var value in values)
                {
                    yield return value;
                }
            }
        }

        public override IEnumerable<T[]> FetchValuesBatch(ISerializeTimeValue<T> builder)
        {
            var zipped = Zipped();
            if (zipped != null)
            {
                var values = GZipFetchValuesBatchLazy(zipped, builder);
                foreach (var value in values)
                {
                    yield return value;
                }
            }

            var current = Current();
            if (current != null)
            {
                var values = FetchValuesBatchLazy(current, builder);
                yield return values;
            }
        }

        public override void Compact(ISerializeTimeValue<T> builder)
        {
            using (var l = new FileLockV2(_dir, _shardName))
            {
                var current = Current();
                if (current != null)
                {
                    AppendToZipArchive(stream =>
                    {
                        var values = FetchValuesBatchLazy(current, builder);
                        foreach (var value in values)
                            builder.Serialize(value, stream);
                    });

                    File.Delete(current.FullName + Consts.Header);
                    File.Delete(current.FullName);
                }
            }
        }

        void AppendToZipArchive(Action<Stream> callback)
        {
            using (var zipFile = Open_Stream_For_Writing(Path.Combine(_dir.FullName, _shardName + ".zip")))
            using (var zipArchive = new ZipArchive(zipFile, ZipArchiveMode.Update))
            {
                var nextNumber = zipArchive.Entries.Where(r => r.Name.StartsWith(_shardName)).Select(s => int.Parse(s.Name.Split('.')[1])).DefaultIfEmpty(-1).Max() + 1;
                var entry = zipArchive.CreateEntry(_shardName + "." + nextNumber);
                using (var fileEntry = entry.Open())
                {
                    callback(fileEntry);
                }
            }
        }

        IEnumerable<T[]> GZipFetchValuesBatchLazy(FileInfo fileInfo, ISerializeTimeValue<T> builder)
        {
            using (var zipFile = Open_Stream_For_Reading(fileInfo.FullName))
            using (var zipArchive = new ZipArchive(zipFile, ZipArchiveMode.Read))
            {
                foreach (var dataEntry in zipArchive.Entries.Where(r => r.Name.StartsWith(_shardName)).OrderBy(r => int.Parse(r.Name.Split('.')[1])))
                {
                    using (var data = dataEntry.Open())
                    using (var buff = new BufferedStream(data, 1024 * 512))
                    {
                        yield return builder.DeserializeAll(buff);
                    }
                }
            }
        }

        IEnumerable<T> GZipFetchValuesLazy(FileInfo fileInfo, ISerializeTimeValue<T> builder)
        {
            using (var zipFile = Open_Stream_For_Reading(fileInfo.FullName))
            using (var zipArchive = new ZipArchive(zipFile, ZipArchiveMode.Read))
            {
                foreach (var dataEntry in zipArchive.Entries.Where(r => r.Name.StartsWith(_shardName)).OrderBy(r => int.Parse(r.Name.Split('.')[1])))
                {
                    using (var data = dataEntry.Open())
                    {
                        while (true)
                        {
                            T value = default(T);
                            try
                            {
                                value = builder.Deserialize(data);
                            }
                            catch
                            {
                                break;
                            }
                            yield return value;
                        }
                    }
                }
            }
        }


        FileInfo Zipped()
        {
            return _dir.GetFiles(_shardName + ".zip").SingleOrDefault();
        }

    }


}
