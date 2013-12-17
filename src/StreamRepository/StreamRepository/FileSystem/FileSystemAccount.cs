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
        FileSystemFactory _factory;
        FileSystemShardingStrategy _defaultStrategy;


        public FileSystemAccount(string directoryPath, FileSystemFactory factory, FileSystemShardingStrategy defaultStrategy )
        {
            _factory = factory;
            _defaultStrategy = defaultStrategy;
            _directory = new DirectoryInfo(directoryPath);
            if (!_directory.Exists)
                _directory.Create();
        }


        public override Repository BuildRepository(string streamName)
        {
            var directory = new DirectoryInfo(Path.Combine(_directory.FullName, streamName));
            if (!directory.Exists)
                directory.Create();

            return _factory.OperOrCreate(directory, _defaultStrategy);
        }

        public override void Reset()
        {
            if (_directory.Exists)
                _directory.Delete(true);
            _directory.Create();
        }
        public override IEnumerable<string> GetStreams()
        {
            return _directory.EnumerateDirectories().Select(s => s.Name);
        }
    }

}
