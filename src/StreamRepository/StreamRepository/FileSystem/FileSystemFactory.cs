using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileSystemFactory
    {
        static Func<string, IEnumerable<FileInfo>, ShardingStrategy> _buildStrategy;
        public static readonly string Sharding = "sharding-";

        static FileSystemFactory()
        {
            _buildStrategy = (id, files) => 
            {
                switch (id)
                { 
                    case "9C2880C1-16D7-4D90-8D37-CC3D7231EAB0" :
                        return new FileSystemPerYearShardingStrategy(files);
                    case "CAABA129-479F-4F36-B5B9-B08C59EEB6CF":
                        return new FileSystemPerMonthShardingStrategy(files);
                }
                return new NoShardingStrategy();
            };
        }

        public FileSystemRepository OperOrCreate(DirectoryInfo directory, string id)
        {
            ShardingStrategy sharding = null;
            var files = directory.GetFiles().ToList();
            var dataFiles = files.Where(f => !f.Name.StartsWith(Sharding)).ToList();

            if (!files.Any())
            {
                var path = Path.Combine(directory.FullName, Sharding + id);
                using (new FileInfo(path).Create()) ;
            }
            else
            {
                var factory = files.Single(f => f.Name.StartsWith(Sharding)).Name;

                int spearatorIndex = factory.IndexOf('-');
                id = factory.Substring(spearatorIndex + 1);
                sharding = _buildStrategy(id, dataFiles);
            }

            return new FileSystemRepository(directory, sharding);
        }

        bool Stream_does_not_exists(DirectoryInfo directory)
        {
            return !directory.Exists;
        }

    }

}
