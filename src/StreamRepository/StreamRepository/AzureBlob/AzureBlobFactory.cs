using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.Azure
{
    public class Consts
    {
        public const string Sharding = "sharding-";

    }
    public class AzureBlobFactory<T> where T : ITimeValue
    {
        IEnumerable<AzureBlobShardingStrategy<T>> _strategies;
        IBuildStuff _builder;

        public AzureBlobFactory(IEnumerable<AzureBlobShardingStrategy<T>> strategies, IBuildStuff builder)
        {
            _strategies = strategies;
            _builder = builder;
        }


        public AzureBlobRepository<T> OperOrCreate(CloudBlobDirectory directory, AzureBlobShardingStrategy<T> defaultShardingStrategy)
        {
            AzureBlobShardingStrategy<T> sharding = defaultShardingStrategy;
            var blobs = directory.ListBlobs().ToList();
            var dataBlobs = blobs.Where(s => !s.Uri.Segments.Last().StartsWith(Consts.Sharding)).ToList();

            if (!blobs.Any())
            {
                var id = GetId(sharding);
                var factoryBlob = directory.GetPageBlobReference(Get_Factory_Blob(directory, id.ToString()));
                factoryBlob.Create(0);
            }
            else
            {
                var factory = blobs.Select(s => s.Uri.Segments.Last()).Single(b => b.StartsWith(Consts.Sharding));

                int spearatorIndex = factory.IndexOf('-');
                var id = factory.Substring(spearatorIndex + 1);
                sharding = BuildShardingStrategy(id);
            }

            return new AzureBlobRepository<T>(directory, sharding, _builder);
        }

        string Get_Factory_Blob(CloudBlobDirectory directory, string factoryId)
        {
            return new Uri(directory.Uri, Consts.Sharding + factoryId).ToString();
        }


        Guid GetId(object obj)
        {
            var att = obj.GetType().GetCustomAttributes(typeof(GuidAttribute), true)
                .OfType<GuidAttribute>().FirstOrDefault();
            if (att == null)
                throw new Exception();

            return Guid.Parse(att.Value);
        }


        AzureBlobShardingStrategy<T> BuildShardingStrategy(string id)
        {
            foreach (var s in _strategies)
                if (GetId(s).ToString() == id)
                    return s;

            throw new Exception("Strategy not found");
        }
    }

}
