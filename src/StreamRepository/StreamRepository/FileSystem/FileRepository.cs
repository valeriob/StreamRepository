﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileRepository : Repository
    {
        Dictionary<int, FileInfo> _yearCache;
        Func<int, string> _logFileName = year => string.Format("{0}.dat", year);
        Func<int, string> _logObsoleteFileName = year => string.Format("{0}.dat", year);
        DirectoryInfo _folder;


        public FileRepository(DirectoryInfo folder)
        {
            _folder = folder;
            if (!folder.Exists)
                folder.Create();
            _yearCache = new Dictionary<int, FileInfo>();
        }



        public override void Append_Values(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            foreach (var year in values.GroupBy(g => g.Item1.Year))
            {
                var header = Read_Header(year.Key);

                using (var stream = Open_Stream_For_Writing(year.Key))
                {
                    var tail = header.Index;

                    using (var buffer = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(buffer))
                            foreach (var value in year)
                                new FramedValue(value.Item1, value.Item2, value.Item3).Serialize(writer);

                        stream.Seek(tail, SeekOrigin.Begin);

                        int writtenBytes = year.Count() * FramedValue.SizeInBytes();
                        stream.Write(buffer.GetBuffer(), 0, writtenBytes);

                        tail = tail + writtenBytes;
                    }

                    using (var buffer = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(buffer))
                        {
                            header.Index = tail;
                            header.Timestamp = DateTime.Now;
                            header.Serialize(writer);
                        }

                        stream.Seek(0, SeekOrigin.Begin);

                        stream.Write(buffer.GetBuffer(), 0, StreamHeader.SizeInBytes());
                    }
                    stream.Flush();
                }
            }
        }

        public override void Hint_Year_Samples(int year, int samples)
        {
            using (var file = Get_Year_With_Caching(year).OpenWrite())
            {
                var size = FramedValue.SizeInBytes() * samples;
                file.SetLength(size);
            }
        }

        public override IEnumerable<RecordValue> Get_Values(DateTime? from, DateTime? to)
        {
            var years = Get_Years();
            if (from.HasValue)
                years = years.Where(y => y >= from.Value.Year);
            if (to.HasValue)
                years = years.Where(y => y <= to.Value.Year);

            foreach (var year in years)
            {
                using (var file = Open_Stream_For_Reading(year))
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

        public override IEnumerable<byte[]> Get_Raw_Values(DateTime? from = null, DateTime? to = null)
        {
            var years = Get_Years();
            if (from.HasValue)
                years = years.Where(y => y >= from.Value.Year);
            if (to.HasValue)
                years = years.Where(y => y <= to.Value.Year);

            foreach (var year in years)
            {
                using (var file = Open_Stream_For_Reading(year))
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


        IEnumerable<int> Get_Years()
        {
            return _folder.GetFiles().Select(f => new
            {
                f,
                f.Name,
                Year = Convert.ToInt32(Path.GetFileNameWithoutExtension(f.FullName))
            }).OrderBy(d => d.Year)
             .Select(s => s.Year);
        }

        StreamHeader Read_Header(int year)
        {
            try
            {
                using (var stream = Open_Stream_For_Reading(year))
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

        Stream Open_Stream_For_Reading(int year)
        {
            return Get_Year_With_Caching(year).OpenRead();
        }

        Stream Open_Stream_For_Writing(int year)
        {
            //return new FileStream(Get_Year_With_Caching(year).FullName, FileMode.Open, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough | FileOptions.SequentialScan);
            return new FileStream_With_Hard_Flush(Get_Year_With_Caching(year).FullName, FileMode.Open);
            //return Get_Year_With_Caching(year).Open(FileMode.Open);
        }


        FileInfo Get_Year(int year)
        {
            foreach (var file in _folder.GetFiles())
                if (file.Name == _logFileName(year))
                    return file;

            var path = Path.Combine(_folder.FullName, _logFileName(year));

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

        FileInfo Get_Year_With_Caching(int year)
        {
            FileInfo file = null;
            if (!_yearCache.TryGetValue(year, out file))
            {
                file = Get_Year(year);
                _yearCache[year] = file;
            }

            return file;
        }

    }


    public class FileStream_With_Hard_Flush : FileStream
    {
        public FileStream_With_Hard_Flush(string path, FileMode fileMode)
            : base(path, fileMode)
        { }

        public override void Flush()
        {
            base.Flush(true);
            // FlushFileBuffers(base.SafeFileHandle.DangerousGetHandle());
        }

        [DllImport("kernel32.dll")]
        static extern bool FlushFileBuffers(IntPtr hFile);
    }
}
