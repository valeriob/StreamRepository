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

namespace Stress
{
    class Program
    {
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
            //var filePath = @"d:\Amadori";
            //Account account = new FileSystemAccount(filePath);

            /*---------------  AZURE  -------------*/
            var azureAccount = new CloudStorageAccount(new StorageCredentials("valeriob", "2SzgTAaG11U0M1gQ19SNus/vv1f0efwYOwZHL1w9YhTKEYsU1ul+s/ke92DOE1wIeCKYz5CuaowtDceUvZW2Rw=="), true);
            var blobClient = azureAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("onenergy-amadori");
            container.CreateIfNotExists();
            Account account = new BlobAccount(container);

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
            account.Write_Streams(1, 5);
            watch.Stop();
            Console.WriteLine(watch.Elapsed);

            //watch = Stopwatch.StartNew();
            //account.Read_Streams();
            //watch.Stop();
            //Console.WriteLine(watch.Elapsed);

            var streams = account.Get_Streams().ToArray();
            var random = new Random();
            while (true)
            {
                var rep = account.Build_Repository(streams[random.Next(streams.Length)]);

                watch = Stopwatch.StartNew();
                var test = rep.Get_Raw_Values(new DateTime(2012, 1, 1)).ToList();
                    //.Where(v => v.Timestamp > new DateTime(2012, 5, 1)).ToList();
                watch.Stop();

                Console.WriteLine(watch.Elapsed);
            }

            Console.ReadLine();
        }

    }

}
