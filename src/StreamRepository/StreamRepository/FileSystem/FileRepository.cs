using System;
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

        public override void Hint_Year_Samples(int year, int samples)
        {
            using (var file = Get_Year(year).OpenWrite())
            {
                var size = FramedValue.SizeInBytes() * samples;
                file.SetLength(size);
            }
        }

        protected override Stream Open_Stream_For_Reading(int year)
        {
            return Get_Year_With_Caching(year).OpenRead();
        }
        protected override Stream Open_Stream_For_Writing(int year)
        {
            //return new FileStream(Get_Year_With_Caching(year).FullName, FileMode.Open, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough | FileOptions.SequentialScan);
            return new FileStream_With_Hard_Flush(Get_Year_With_Caching(year).FullName, FileMode.Open);
            //return Get_Year_With_Caching(year).Open(FileMode.Open);
        }

        protected override IEnumerable<int> Get_Years()
        {
            return _folder.GetFiles().Select(f => new
             {
                 f,
                 f.Name,
                 Year = Convert.ToInt32(Path.GetFileNameWithoutExtension(f.FullName))
             }).OrderBy(d => d.Year)
             .Select(s => s.Year);
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
