using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StreamRepository;
using StreamRepository.FileSystem;
using StreamRepository.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace Stress
{
    class Program
    {
        static readonly int Uno = 365 * 24 * 60 * 60;
        static readonly int OgniQuartoDiOra = 15 * 60;
        static readonly int OgniMinuto = 60;

        static void Main(string[] args)
        {
            Stopwatch watch = null;

            //watch = Stopwatch.StartNew();
            //int count = 0;
            //for (int i = 0; i < int.MaxValue; i++)
            //    count++;
            //watch.Stop();
            //Console.WriteLine("cpu speed : " + watch.Elapsed);

            /*---------------     FS -------------*/
            var filePath = @"c:\temp\Amadori";
            var ff = new FileSystemFactory(new FileSystemShardingStrategy[] { new FileSystemPerYearShardingStrategy(), new FileSystemPerMonthShardingStrategy() });
            Account account = new FileSystemAccount(filePath, ff, new FileSystemPerYearShardingStrategy());

            /*---------------  AZURE  -------------*/
            //var azureAccount = new CloudStorageAccount(new StorageCredentials("valeriob", "2SzgTAaG11U0M1gQ19SNus/vv1f0efwYOwZHL1w9YhTKEYsU1ul+s/ke92DOE1wIeCKYz5CuaowtDceUvZW2Rw=="), true);
            //var tableClient = azureAccount.CreateCloudTableClient();
            //var blobClient = azureAccount.CreateCloudBlobClient();
            //var container = blobClient.GetContainerReference("onenergy-amadori");
            //container.CreateIfNotExists();
            //var bf = new AzureBlobFactory(new AzureBlobShardingStrategy[] { new AzureBlobPerYearShardingStrategy(), new AzureBlobPerMonthShardingStrategy() });
            //Account account = new AzureBlobAccount(container, bf);

            //var indexTable = tableClient.GetTableReference("index");
            //var operation = TableOperation.InsertOrReplace(null);
            //indexTable.Execute(operation);

            //while (true)
            //{
            //    watch = Stopwatch.StartNew();
            //    using (var buffer = new MemoryStream())
            //    {
            //        var blob = blobClient.GetBlobReferenceFromServer(new Uri("http://valeriob.blob.core.windows.net/onenergy-amadori/11a6f393-c4b9-42ba-8a18-bccd2adcbbee/2008"));
            //        blob.DownloadToStream(buffer);
            //    }
            //    watch.Stop();
            //    Console.WriteLine(watch.Elapsed);
            //}
            //Console.ReadLine();
            //return;



            watch = Stopwatch.StartNew();
            account.Reset();
            account.Write_Streams(10, 5, OgniMinuto);
            watch.Stop();
            Console.WriteLine(watch.Elapsed);

            watch = Stopwatch.StartNew();
            account.Read_Streams();
            watch.Stop();
            Console.WriteLine(watch.Elapsed);

            Console.WriteLine("hit enter to loop reading");
            Console.ReadLine();


            var streams = account.Get_Streams().ToArray();
            var random = new Random();
            var id = random.Next(streams.Length);
            var stream = streams[id];
            var rep = account.Build_Repository(stream);
            while (true)
            {
               

                watch = Stopwatch.StartNew();
                var count = rep.Get_Raw_Values(new DateTime(2012, 1, 1)).Count();
                watch.Stop();

                Console.WriteLine("Raw : " + watch.Elapsed);

                //watch = Stopwatch.StartNew();
                //rep.Get_Values(new DateTime(2012, 1, 1)).ToList();
                //watch.Stop();

                //Console.WriteLine("'Deserialized' : " + watch.Elapsed);
            }

            Console.ReadLine();
        }


    }


    public class IndexEntity : TableEntity
    {
        public IndexEntity(string streamId, string blobUri)
        {
            this.PartitionKey = streamId;
            this.RowKey = blobUri;
        }

        public IndexEntity() { }

        public string StreamId { get; set; }
        public string BlobUri { get; set; }
        public int Size { get; set; }
    }
}
