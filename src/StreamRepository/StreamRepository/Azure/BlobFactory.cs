using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.Azure
{
    public class BlobFactory
    {
        static Func<string, IEnumerable<IListBlobItem>, ShardingStrategy> _buildStrategy;
        public static readonly string Sharding = "sharding-";

        static BlobFactory()
        {
            _buildStrategy = (id, blobs) => 
            {
                switch (id)
                { 
                    case "0F87CB4A-13D4-4991-98FA-58EA5B95DE73" :
                        return new BlobPerYearShardingStrategy(blobs);
                    case "1D267B88-B620-4584-8C17-46B2B648FB20":
                         return new BlobPerMonthShardingStrategy(blobs);
                }
                return new NoShardingStrategy();
            };
        }

        public AzureBlobRepository OperOrCreate(CloudBlobDirectory directory, ShardingStrategy sharding)
        {
            var blobs = directory.ListBlobs().ToList();

            if (!blobs.Any())
            {
                var id = sharding.GetType().GetAttribute<System.Runtime.InteropServices.GuidAttribute>().Value;
                var factoryBlob = directory.GetPageBlobReference(Get_Factory_Blob(directory, id));
                factoryBlob.Create(0);
            }
            else
            {
                var factory = blobs.Select(s=> s.Uri.Segments.Last())
                    .Single(b => b.StartsWith(Sharding));

                int spearatorIndex = factory.IndexOf('-');
                var id = factory.Substring(spearatorIndex + 1);
                sharding = _buildStrategy(id, blobs);
            }

            return new AzureBlobRepository(directory, sharding);
        }

        string Get_Factory_Blob(CloudBlobDirectory directory, string factoryId)
        {
            return new Uri(directory.Uri, Sharding + factoryId).ToString();
        }

    }

}
