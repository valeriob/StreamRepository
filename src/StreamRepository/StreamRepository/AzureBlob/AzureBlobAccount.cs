using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using StreamRepository.Azure;
using EventStore.BufferManagement;

namespace StreamRepository.Azure
{
    public class AzureBlobAccount : Account
    {
        CloudBlobContainer _container;
        AzureBlobFactory _factory;

        public AzureBlobAccount(CloudBlobContainer container, AzureBlobFactory factory)
        {
            _container = container;
            _factory = factory;
        }

        public override Repository BuildRepository(string streamName)
        {
            var directory = _container.GetDirectoryReference(streamName);
            var defaultSharding = new AzureBlobPerYearShardingStrategy();

            return _factory.OperOrCreate(directory, defaultSharding);
        }
        public override IEnumerable<string> GetStreams()
        {
            return _container.ListBlobs().Select(b => b.Uri.Segments.Last());
        }

        public override void Reset()
        {
            //_container.DeleteIfExists();
            //_container = new CloudBlobContainer(_container.Uri);
            //while (_container.Exists())
            //    System.Threading.Thread.Sleep(100);

            //_container.CreateIfNotExists();

            //for (int i = 2000; i < 2013; i++)
            //{
            //    var year = _container.GetSubdirectoryReference(i+"");
            //    foreach(var blob in year.ListBlobs())
            //        _container.ServiceClient.GetBlobReferenceFromServer(blob.Uri).DeleteIfExists();
            //}
        }
    }
}
