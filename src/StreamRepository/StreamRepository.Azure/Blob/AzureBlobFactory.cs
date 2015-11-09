using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;

namespace StreamRepository.Azure.Blob
{
    public class Consts
    {
        public const string Sharding = "sharding-";

    }
    public class AzureBlobFactory<T>
    {
        IEnumerable<AzureBlobShardingStrategy<T>> _strategies;
        ISerializeTimeValue<T> _builder;

        public AzureBlobFactory(IEnumerable<AzureBlobShardingStrategy<T>> strategies, ISerializeTimeValue<T> builder)
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
                var id = sharding.GetId();
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



        AzureBlobShardingStrategy<T> BuildShardingStrategy(string id)
        {
            foreach (var s in _strategies)
                if (s.GetId() == id)
                    return s;

            throw new Exception("Strategy not found");
        }
    }

}
