using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class Consts
    {
        public const string Sharding = "sharding-";
    }

    public class FileSystemFactory<T> where T : ITimeValue
    {
       
        //IEnumerable<FileSystemShardingStrategy> _strategies;
        Dictionary<string, FileSystemShardingStrategy<T>> _strategies;
        IBuildStuff _builder;

        public FileSystemFactory(IEnumerable<FileSystemShardingStrategy<T>> strategies, IBuildStuff builder)
        {
            _strategies = strategies.ToDictionary(d => GetId(d).ToString(), r=> r);
            _builder = builder;
        }

        public FileSystemRepository<T> OperOrCreate(DirectoryInfo directory, FileSystemShardingStrategy<T> defaultShardingStrategy)
        {
            var sharding = defaultShardingStrategy;

            var files = directory.GetFiles().ToList();
            var dataFiles = files.Where(f => !f.Name.StartsWith(Consts.Sharding)).ToList();

            if (!files.Any())
            {
                var id = GetId(sharding);
                var path = Path.Combine(directory.FullName, Consts.Sharding + id);
                using (new FileInfo(path).Create()) ;
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

        Guid GetId(object obj)
        {
            var att = obj.GetType().GetCustomAttributes(typeof(GuidAttribute), true)
                .OfType<GuidAttribute>().FirstOrDefault();
            if (att == null)
                throw new Exception();

            return Guid.Parse(att.Value);
        }


        FileSystemShardingStrategy<T> BuildShardingStrategy(string id)
        {
            return _strategies[id];
        }

        //FileSystemShardingStrategy BuildShardingStrategy(string id)
        //{
        //    foreach (var s in _strategies)
        //        if (GetId(s).ToString() == id)
        //            return s.v;

        //    throw new Exception("Strategy not found");
        //}

    }

}
