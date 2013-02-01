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
        static Func<string, CloudBlobDirectory, ShardingStrategy> _buildStrategy;

        static BlobFactory()
        {
            _buildStrategy = (id, directory) => 
            {
                switch (id)
                { 
                    case "0F87CB4A-13D4-4991-98FA-58EA5B95DE73" :
                        return new BlobPerYearShardingStrategy(directory);
                    case "1D267B88-B620-4584-8C17-46B2B648FB20":
                         return new BlobPerMonthShardingStrategy(directory);
                }
                return new NoShardingStrategy();
            };
        }

        public AzureBlobRepository OperOrCreate(CloudBlobDirectory directory, ShardingStrategy sharding)
        {
            var indexBlob = directory.GetPageBlobReference(NamingUtilities.Get_Index_File(directory));

            if (Stream_does_not_exists(directory))
            {
                var state = new PageBlobState(directory, NamingUtilities.Get_Index_File(directory));
                state.Create();
                state.Open();
                var id = sharding.GetType().GetAttribute<System.Runtime.InteropServices.GuidAttribute>().Value;
                state.Append(id);
            }
            else
            {
                using (var stream = indexBlob.OpenRead())
                using (var reader = new StreamReader(stream))
                {
                    var lines = reader.ReadToEnd().Split(new[] { Environment.NewLine }, StringSplitOptions.None);
                    var id = lines.First();
                    sharding = _buildStrategy(id, directory);
                }
            }

            return new AzureBlobRepository(directory, sharding);
        }


        bool Stream_does_not_exists(CloudBlobDirectory directory)
        {
            return !directory.ListBlobs().Any();
        }

    }

}
