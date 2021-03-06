﻿using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileSystemFactory
    {
        public static readonly string Sharding = "sharding-";
        //IEnumerable<FileSystemShardingStrategy> _strategies;
        Dictionary<string, FileSystemShardingStrategy> _strategies;
        IBuildStuff _builder;

        public FileSystemFactory(IEnumerable<FileSystemShardingStrategy> strategies, IBuildStuff builder)
        {
            _strategies = strategies.ToDictionary(d => GetId(d).ToString(), r=> r);
            _builder = builder;
        }

        public FileSystemRepository OperOrCreate(DirectoryInfo directory, FileSystemShardingStrategy defaultShardingStrategy)
        {
            var sharding = defaultShardingStrategy;

            var files = directory.GetFiles().ToList();
            var dataFiles = files.Where(f => !f.Name.StartsWith(Sharding)).ToList();

            if (!files.Any())
            {
                var id = GetId(sharding);
                var path = Path.Combine(directory.FullName, Sharding + id);
                using (new FileInfo(path).Create()) ;
            }
            else
            {
                var factory = files.Single(f => f.Name.StartsWith(Sharding)).Name;

                int spearatorIndex = factory.IndexOf('-');
                var id = factory.Substring(spearatorIndex + 1);
                sharding = BuildShardingStrategy(id);
            }

            return new FileSystemRepository(directory, sharding, _builder);
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


        FileSystemShardingStrategy BuildShardingStrategy(string id)
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
