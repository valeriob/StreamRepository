using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.Azure
{
    public interface Cache
    {
        bool Contains(string name);
        Stream Get(string name);
        CanReadValues GetReader(string name);
        void Put(Stream stream, string name);
        void Invalidate(string name);
    }

    public class FileSystemCache : Cache
    {
        DirectoryInfo _directory;
        public FileSystemCache(DirectoryInfo directory)
        {
            _directory = directory;
        }


        public bool Contains(string name)
        {
            return _directory.EnumerateFiles("name").Any();
        }

        public Stream Get(string name)
        {
            var path = Path.Combine(_directory.FullName, name);
            return File.OpenRead(path);
        }

        public void Put(Stream stream, string name)
        {
            var path = Path.Combine(_directory.FullName, name);
            using (var dest = File.Open(path, FileMode.OpenOrCreate))
                stream.CopyTo(dest);
        }


        public void Invalidate(string name)
        {
            var path = Path.Combine(_directory.FullName, name);
            File.Delete(path);
        }


        public CanReadValues GetReader(string name)
        {
            throw new NotImplementedException();
        }
    }
}
