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
                        return new FilePerYearShardingStrategy(files);
                    case "CAABA129-479F-4F36-B5B9-B08C59EEB6CF":
                        return new FilePerMonthShardingStrategy(files);
                }
                return new NoShardingStrategy();
            };
        }

        public FileSystemRepository OperOrCreate(DirectoryInfo directory, ShardingStrategy sharding)
        {
            var files = directory.GetFiles().ToList();

            if (!files.Any())
            {
                var id = sharding.GetType().GetAttribute<System.Runtime.InteropServices.GuidAttribute>().Value;
                using (new FileInfo(Path.Combine(directory.FullName, Sharding + id)).Create()) ;
            }
            else
            {
                var factory = files.Single(f => f.Name.StartsWith(Sharding)).Name;
                var dataFiles = files.Where(f => !f.Name.StartsWith(Sharding)).ToList();

                int spearatorIndex = factory.IndexOf('-');
                var id = factory.Substring(spearatorIndex + 1);
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
