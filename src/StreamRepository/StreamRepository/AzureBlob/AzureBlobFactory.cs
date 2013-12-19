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
    public class AzureBlobFactory
    {
        public static readonly string Sharding = "sharding-";

        IEnumerable<AzureBlobShardingStrategy> _strategies;
        IBuildStuff _builder;

        public AzureBlobFactory(IEnumerable<AzureBlobShardingStrategy> strategies, IBuildStuff builder)
        {
            _strategies = strategies;
            _builder = builder;
        }


        public AzureBlobRepository OperOrCreate(CloudBlobDirectory directory, AzureBlobShardingStrategy defaultShardingStrategy)
        {
            AzureBlobShardingStrategy sharding = defaultShardingStrategy;
            var blobs = directory.ListBlobs().ToList();
            var dataBlobs = blobs.Where(s => !s.Uri.Segments.Last().StartsWith(Sharding)).ToList();

            if (!blobs.Any())
            {
                var id = GetId(sharding);
                var factoryBlob = directory.GetPageBlobReference(Get_Factory_Blob(directory, id.ToString()));
                factoryBlob.Create(0);
            }
            else
            {
                var factory = blobs.Select(s=> s.Uri.Segments.Last()).Single(b => b.StartsWith(Sharding));

                int spearatorIndex = factory.IndexOf('-');
                var id = factory.Substring(spearatorIndex + 1);
                sharding = BuildShardingStrategy(id);
            }

            return new AzureBlobRepository(directory, sharding, _builder);
        }

        string Get_Factory_Blob(CloudBlobDirectory directory, string factoryId)
        {
            return new Uri(directory.Uri, Sharding + factoryId).ToString();
        }


        Guid GetId(object obj)
        {
            var att = obj.GetType().GetCustomAttributes(typeof(GuidAttribute), true)
                .OfType<GuidAttribute>().FirstOrDefault();
            if (att == null)
                throw new Exception();

            return Guid.Parse(att.Value);
        }


        AzureBlobShardingStrategy BuildShardingStrategy(string id)
        {
            foreach (var s in _strategies)
                if (GetId(s).ToString() == id)
                    return s;

            throw new Exception("Strategy not found");
        }
    }

}
