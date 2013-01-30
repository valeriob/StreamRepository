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
        static Func<string, DirectoryInfo, ShardingStrategy> _buildStrategy;

        static FileSystemFactory()
        {
            _buildStrategy = (id, directory) => 
            {
                switch (id)
                { 
                    case "9C2880C1-16D7-4D90-8D37-CC3D7231EAB0" :
                        return new FilePerYearShardingStrategy(directory);
                    case "CAABA129-479F-4F36-B5B9-B08C59EEB6CF":
                         return new FilePerMonthShardingStrategy(directory);
                }
                return new NoShardingStrategy();
            };
        }

        public FileSystemRepository OperOrCreate(DirectoryInfo directory, ShardingStrategy sharding)
        {
            string index = NamingUtilities.Get_Index_File(directory);

            if (Stream_does_not_exists(directory))
            {
                directory.Create();
                var id = sharding.GetType().GetAttribute<System.Runtime.InteropServices.GuidAttribute>().Value;
                File.AppendAllText(index, id);
            }
            else
            {
                var lines = File.ReadAllLines(index);
                var id = lines.First();
                sharding = _buildStrategy(id, directory);
            }
            return new FileSystemRepository(directory, sharding);
        }

        bool Stream_does_not_exists(DirectoryInfo directory)
        {
            return !directory.Exists;
        }

    }

}
