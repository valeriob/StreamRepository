﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using StreamRepository.Azure;
using EventStore.BufferManagement;

namespace StreamRepository.Azure
{
    public class BlobAccount : Account
    {
        CloudBlobContainer _container;
        BlobFactory _factory;

        public BlobAccount(CloudBlobContainer container, BlobFactory factory)
        {
            _container = container;
            _factory = factory;
        }

        public override Repository Build_Repository(string streamName)
        {
            var directory = _container.GetDirectoryReference(streamName);
            var defaultSharding = "0F87CB4A-13D4-4991-98FA-58EA5B95DE73";

            return _factory.OperOrCreate(directory, defaultSharding);
        }
        public override IEnumerable<string> Get_Streams()
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
