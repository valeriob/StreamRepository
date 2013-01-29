using EventStore.BufferManagement;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileSystemAccount : Account
    {
        DirectoryInfo _directory;

        public FileSystemAccount(string directoryPath)
        {
            _directory = new DirectoryInfo(directoryPath);
            if (!_directory.Exists)
                _directory.Create();
        }



        public override Repository Build_Repository(string streamName)
        {
            var directory = new DirectoryInfo(Path.Combine(_directory.FullName, streamName));
            var sharding = new FilePerYearShardingStrategy(directory);
            return new FileSystemRepository(directory, sharding);
        }

        public override void Reset()
        {
            if (_directory.Exists)
                _directory.Delete(true);
            _directory.Create();
        }
        public override IEnumerable<string> Get_Streams()
        {
            return _directory.EnumerateDirectories().Select(s => s.Name);
        }
    }

}
