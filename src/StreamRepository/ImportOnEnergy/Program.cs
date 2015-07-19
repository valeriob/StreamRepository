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
            var filePath = @"f:\temp\Amadori";
            var ff = new FileSystemFactory<InputValue>(new FileSystemShardingStrategy<InputValue>[] { new FileSystemPerYearShardingStrategy<InputValue>(), new FileSystemPerMonthShardingStrategy<InputValue>() }, new InputValueBuilder());
            Account<InputValue> account = new FileSystemAccount<InputValue>(filePath, ff, new FileSystemPerYearShardingStrategy<InputValue>());

            /*---------------  AZURE  -------------*/
            //var azureAccount = new CloudStorageAccount(new StorageCredentials("onenergy", "SOKk3P1fokgf9RoxrL/SJ5SIapsMdx6X1vTj7KyB7iMvCp0fcW7qLb53+KjHTDecycf257c2tpCuK5O5OtqQkA=="), true);
            //var tableClient = azureAccount.CreateCloudTableClient();
            //var blobClient = azureAccount.CreateCloudBlobClient();
            //var container = blobClient.GetContainerReference("values-onimenergy");
            //container.CreateIfNotExists();
            //var bf = new AzureBlobFactory(new AzureBlobShardingStrategy[] { new AzureBlobPerYearShardingStrategy(), new AzureBlobPerMonthShardingStrategy() }, new InputValueBuilder());
            //Account account = new AzureBlobAccount(container, bf);

            //RunImport(account);
            account.Read_Streams();
        }


        public static void RunImport(Account<InputValue> account)
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
