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
        BufferPool _bufferPool;

        public FileSystemAccount(string directoryPath)
        {
            _bufferPool = new BufferPool();
            _directory = new DirectoryInfo(directoryPath);
            if (!_directory.Exists)
                _directory.Create();
        }



        public override Repository Build_Repository(string streamName)
        {
            var directory = new DirectoryInfo(Path.Combine(_directory.FullName, streamName));
            return new FileRepository(directory, _bufferPool);
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
