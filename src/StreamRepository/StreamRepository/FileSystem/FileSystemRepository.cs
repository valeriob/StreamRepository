using EventStore.BufferManagement;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileSystemRepository : Repository
    {
        Dictionary<string, FileInfo> _fileCache;
        Func<int, string> _logFileName = year => string.Format("{0}.dat", year);
        Func<int, string> _logObsoleteFileName = year => string.Format("{0}.dat", year);
        DirectoryInfo _directory;
        FileSystemShardingStrategy _sharding;


        public FileSystemRepository(DirectoryInfo folder, FileSystemShardingStrategy sharding)
        {
            _sharding = sharding;
            _directory = folder;
            _fileCache = new Dictionary<string, FileInfo>();
        }


        public async Task AppendValues(IEnumerable<Event> values)
        {
            foreach (var shard in _sharding.Shard(values) )
            {
                var group = shard.GetValues();
                var name = shard.GetName();
                var header = ReadHeader(name);

                using (var stream = Open_Stream_For_Writing(name))
                {
                    var tail = header.Index;

                    //using (var buffer = new BufferPoolStream(new BufferPool()))
                    using(var buffer = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(buffer, Encoding.Default, true))
                            foreach (var value in group)
                            {
                                //var fv = new FramedValue(value.Item1, value.Item2, value.Item3);
                                //fv.Serialize(writer);
                                FramedValue.Serialize(value.Timestamp, value.Value, value.ImportId, writer);
                            }
                        stream.Seek(tail, SeekOrigin.Begin);
                        buffer.Seek(0, SeekOrigin.Begin);
                        int writtenBytes = group.Count() * FramedValue.SizeInBytes();
                        await buffer.CopyToAsync(stream);
                        stream.Flush();

                        tail = tail + writtenBytes;
                    }

                    header.Index = tail;
                    header.Timestamp = DateTime.Now;
                   
                }
                await WriteHeader(header, name);
            }
        }

        public void Hint_Sampling_Period(int samplingPeriodInSeconds)
        {
            // TODO
            //using (var file = Get_Year_With_Caching(year).OpenWrite())
            //{
            //    var size = FramedValue.SizeInBytes() * samples;
            //    file.SetLength(size);
            //}
        }

        public IEnumerable<RecordValue> Get_Values(DateTime? from, DateTime? to)
        {
            var files = _directory.GetFiles();

            foreach (var shard in _sharding.GetShards(files, from, to))
            {
                using (var file = Open_Stream_For_Reading(shard.GetName()))
                {
                    // TODO read obsoleted log, and enrich value.

                    var reader = new BinaryReader(file);

                    var header = StreamHeader.Deserialize(reader);

                    file.Seek(StreamHeader.SizeInBytes(), SeekOrigin.Begin);
                    while (file.Position < header.Index)
                        //yield return FramedValue.Deserialize(reader, file.Position);
                        yield return FramedValue.Deserialize(file, file.Position);
                }
            }
        }

        public IEnumerable<byte[]> Get_Raw_Values(DateTime? from = null, DateTime? to = null)
        {
            var files = _directory.GetFiles();
            foreach (var shard in _sharding.GetShards(files, from, to))
            {
                using (var file = Open_Stream_For_Reading(shard.GetName()))
                {
                    // TODO read obsoleted log, and enrich value.

                    var reader = new BinaryReader(file);

                    var header = StreamHeader.Deserialize(reader);

                    file.Seek(StreamHeader.SizeInBytes(), SeekOrigin.Begin);
                    while (file.Position < header.Index)
                    {
                        //yield return FramedValue.Deserialize(reader, file.Position);
                       // yield return new ArraySegment<byte>(file, (int)file.Position, FramedValue.SizeInBytes());
                        var data = new byte[FramedValue.SizeInBytes()];
                        file.Read(data, 0, data.Length);
                        yield return data;
                    }
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
            catch (System.IO.EndOfStreamException)
            {
                return new StreamHeader() { Index = StreamHeader.SizeInBytes() };
            }
        }

        async Task WriteHeader(StreamHeader header, string name)
        {
            using (var buffer = new BufferPoolStream(new BufferPool()))
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



        public void Reset()
        {
            throw new NotImplementedException();
        }
    }


}
