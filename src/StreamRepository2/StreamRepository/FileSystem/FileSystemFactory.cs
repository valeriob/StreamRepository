using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace StreamRepository.FileSystem
{
    public class Consts
    {
        public const string Sharding = "sharding-";
        public const string Header = ".header";
    }

    public class FileSystemFactory<T> where T : ITimeValue
    {
        Dictionary<string, FileSystemShardingStrategy<T>> _strategies;
        ISerializeTimeValue<T> _builder;

        public FileSystemFactory(IEnumerable<FileSystemShardingStrategy<T>> strategies, ISerializeTimeValue<T> builder)
        {
            _strategies = strategies.ToDictionary(d => d.GetName(), r => r);
            _builder = builder;
        }

        public FileSystemRepository<T> OperOrCreate(DirectoryInfo directory, FileSystemShardingStrategy<T> defaultShardingStrategy)
        {
            var sharding = defaultShardingStrategy;

            var files = directory.GetFiles();

            var shardingName = directory.GetFiles(Consts.Sharding + "*").Select(s => s.Name).SingleOrDefault();
            if (string.IsNullOrEmpty(shardingName))
            {
                shardingName = sharding.GetName();
                var path = Path.Combine(directory.FullName, Consts.Sharding + shardingName);
                using (new FileInfo(path).Create()) { }
            }
            else
            {
                int spearatorIndex = shardingName.IndexOf('-');
                var id = shardingName.Substring(spearatorIndex + 1);
                sharding = BuildShardingStrategy(id);
            }

            return new FileSystemRepository<T>(directory, sharding, _builder);
        }

        bool Stream_does_not_exists(DirectoryInfo directory)
        {
            return !directory.Exists;
        }


        FileSystemShardingStrategy<T> BuildShardingStrategy(string id)
        {
            return _strategies[id];
        }

    }

    public class GZipFileSystemFactory<T> where T : ITimeValue
    {
        Dictionary<string, FileSystemShardingStrategy<T>> _strategies;
        ISerializeTimeValue<T> _builder;

        public GZipFileSystemFactory(IEnumerable<FileSystemShardingStrategy<T>> strategies, ISerializeTimeValue<T> builder)
        {
            _strategies = strategies.ToDictionary(d => d.GetName(), r => r);
            _builder = builder;
        }

        public FileSystemRepository<T> OperOrCreate(DirectoryInfo directory, FileSystemShardingStrategy<T> defaultShardingStrategy)
        {
            var sharding = defaultShardingStrategy;

            var files = directory.GetFiles().ToList();
            var dataFiles = files.Where(f => !f.Name.StartsWith(Consts.Sharding)).ToList();

            if (!files.Any())
            {
                var id = defaultShardingStrategy.GetName();
                var path = Path.Combine(directory.FullName, Consts.Sharding + id);
                using (new FileInfo(path).Create()) { }
            }
            else
            {
                var factory = files.Single(f => f.Name.StartsWith(Consts.Sharding)).Name;

                int spearatorIndex = factory.IndexOf('-');
                var id = factory.Substring(spearatorIndex + 1);
                sharding = BuildShardingStrategy(id);
            }

            return new FileSystemRepository<T>(directory, sharding, _builder);
        }

        bool Stream_does_not_exists(DirectoryInfo directory)
        {
            return !directory.Exists;
        }


        FileSystemShardingStrategy<T> BuildShardingStrategy(string id)
        {
            return _strategies[id];
        }

    }
}
