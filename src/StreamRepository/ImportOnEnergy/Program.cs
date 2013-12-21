using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using StreamRepository;
using StreamRepository.Azure;
using StreamRepository.FileSystem;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportOnEnergy
{
    class Program
    {
        static void Main(string[] args)
        {
            /*---------------     FS -------------*/
            //var filePath = @"c:\temp\Amadori";
            //var ff = new FileSystemFactory(new FileSystemShardingStrategy[] { new FileSystemPerYearShardingStrategy(), new FileSystemPerMonthShardingStrategy() }, new InputValueBuilder());
            //Account account = new FileSystemAccount(filePath, ff, new FileSystemPerYearShardingStrategy());

            /*---------------  AZURE  -------------*/
            var azureAccount = new CloudStorageAccount(new StorageCredentials("onenergy", "phyi70b6RgGXJYJscDy2kuQiJrPpdON5p3IRezUKpOYWEf+gHmEvbCSjNOYZI0FfosqjzSQeHPQlxLQTTllGVg=="), true);
            var tableClient = azureAccount.CreateCloudTableClient();
            var blobClient = azureAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("values-onimenergy");
            //var container = blobClient.GetContainerReference("test");
            container.CreateIfNotExists();
            var bf = new AzureBlobFactory(new AzureBlobShardingStrategy[] { new AzureBlobPerYearShardingStrategy(), new AzureBlobPerMonthShardingStrategy() }, new InputValueBuilder());
            Account account = new AzureBlobAccount(container, bf);

            //RunImport(account);
            account.Read_Streams();
        }

        public static void WriteTest(Account account)
        {
            var repository = account.BuildRepository("t1");
            var evnts = new List<Event> 
            {
                new Event(DateTime.Now, 1, 1)
            };
            repository.AppendValues(evnts).Wait();
        }

        public static void RunImport(Account account)
        {
            account.Reset();

            var sw = Stopwatch.StartNew();
            Console.WriteLine("Started " + DateTime.Now);

            var cs = "Data Source=vborioni.cloudapp.net,1433;Initial Catalog=OnEnergy;Integrated Security=False;User ID=vborioni;Password=onit!2013;Connect Timeout=15;Encrypt=False;TrustServerCertificate=False";
            var importer = new Importer(account, cs);
            importer.ImportAllStreams();

            sw.Stop();

            Console.WriteLine("It took : {0} to import {1} streams", sw.Elapsed, importer.ImportedStreams);
            Console.ReadLine();
        }
    }

  

}
