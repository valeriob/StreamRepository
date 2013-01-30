﻿using EventStore.BufferManagement;
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

        public FileSystemAccount(string directoryPath, FileSystemFactory factory)
        {
            _factory = factory;
            _directory = new DirectoryInfo(directoryPath);
            if (!_directory.Exists)
                _directory.Create();
        }


        public override Repository Build_Repository(string streamName)
        {
            var directory = new DirectoryInfo(Path.Combine(_directory.FullName, streamName));
            var defaultSharding = new FilePerYearShardingStrategy(directory);

            return _factory.OperOrCreate(directory, defaultSharding);
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
