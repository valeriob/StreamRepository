using EventStore.BufferManagement;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileSystemAccount<T> : Account<T>
    {
        DirectoryInfo _directory;
        FileSystemFactory<T> _factory;
        FileSystemShardingStrategy<T> _defaultStrategy;


        public FileSystemAccount(string directoryPath, FileSystemFactory<T> factory, FileSystemShardingStrategy<T> defaultStrategy )
        {
            _factory = factory;
            _defaultStrategy = defaultStrategy;
            _directory = new DirectoryInfo(directoryPath);
            if (!_directory.Exists)
                _directory.Create();
        }


        public override Repository<T> BuildRepository(string streamName)
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
