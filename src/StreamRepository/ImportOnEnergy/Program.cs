﻿using Microsoft.WindowsAzure.Storage;
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
            //var filePath = @"e:\temp\Amadori";
            //var ff = new FileSystemFactory(new FileSystemShardingStrategy[] { new FileSystemPerYearShardingStrategy(), new FileSystemPerMonthShardingStrategy() });
            //Account account = new FileSystemAccount(filePath, ff, new FileSystemPerYearShardingStrategy());

            /*---------------  AZURE  -------------*/
            var azureAccount = new CloudStorageAccount(new StorageCredentials("onenergy", "phyi70b6RgGXJYJscDy2kuQiJrPpdON5p3IRezUKpOYWEf+gHmEvbCSjNOYZI0FfosqjzSQeHPQlxLQTTllGVg=="), true);
            var tableClient = azureAccount.CreateCloudTableClient();
            var blobClient = azureAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("imola-inputvalues");
            container.CreateIfNotExists();
            var bf = new AzureBlobFactory(new AzureBlobShardingStrategy[] { new AzureBlobPerYearShardingStrategy(), new AzureBlobPerMonthShardingStrategy() });
            Account account = new AzureBlobAccount(container, bf);

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

    public class Importer
    {
        Account _account;
        string _cs;
        IDbConnection _connection;
        public int ImportedStreams { get; private set; }


        public Importer(Account account, string cs)
        {
            _account = account;
            _cs = cs;

            _connection = new System.Data.SqlClient.SqlConnection(cs);
            _connection.Open();
        }

        public void ImportAllStreams()
        {
            var ids = GetStreamIds().OrderBy(d => d).ToList();

            //var po = new ParallelOptions { MaxDegreeOfParallelism = 4 };
            //Parallel.ForEach(ids, po, id => 
            //{
            //    var sw = Stopwatch.StartNew();
            //    ImportStream(id);
            //    sw.Stop();
            //    ImportedStreams++;

            //    Console.WriteLine("Imported stream {0} in {1}", id, sw.Elapsed);
            //});
            foreach (var id in ids)
            {
                var sw = Stopwatch.StartNew();
                ImportStream(id);
                sw.Stop();
                ImportedStreams++;

                Console.WriteLine("Imported stream {0} in {1}", id, sw.Elapsed);
            }
        }

        void ImportStream(int id)
        {
            var repository = _account.BuildRepository(id + "");
            var events = LoadEventsForStream(id);
            repository.AppendValues(events).Wait();
        }

        public IEnumerable<Event> LoadEventsForStream(int id)
        {
            string query = @"SELECT Id, Value, StreamId, ObsolescenceEventId, UTCFrom, IsDeletedValue, ImportEventId, UTCTo from InputValue
                    WHERE StreamId = @StreamId";

            var parameters = new[] { Tuple.Create<string, object>("StreamId", id) };

            var result = new List<Event>();
            using (var cmd = Prepare(_connection, query, parameters))
            using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                {
                    var ev = reader.ToEvent();
                    result.Add(ev);
                }
            return result;
        }

        IEnumerable<int> GetStreamIds()
        {
            string query = "select Id from Stream where IsObsolete = 0";

            var ids = new List<int>();

            using (var cmd = Prepare(_connection, query))
            using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                    ids.Add(reader.GetInt32(0));

            return ids;
        }

        IDbCommand Prepare(IDbConnection con, string query, IEnumerable<Tuple<string, object>> parameters = null)
        {
            var cmd = con.CreateCommand();
            cmd.CommandText = query;
            if (parameters != null)
                foreach (var p in parameters)
                {
                    var par = cmd.CreateParameter();
                    par.Value = p.Item2;
                    par.ParameterName = p.Item1;
                    cmd.Parameters.Add(par);
                }
            return cmd;
        }

    }

    public static class DataReaderExtensions
    {
        public static InputValue ToInputValue(this IDataReader reader)
        {
            return new InputValue
            {
                Id = reader.GetInt64(0),
                Value = reader.GetDouble(1),
                StreamId = reader.GetInt32(2),
                ObsolescenceEventId = reader.GetInt32(3),
                UTCFrom = reader.GetDateTime(4),
                IsDeletedValue = reader.GetBoolean(5),
                ImportEventId = reader.GetInt64(6),
                UTCTo = reader.GetDateTime(7),
            };
        }
        public static Event ToEvent(this IDataReader reader)
        {
            return new Event(reader.GetDateTime(7), reader.GetDouble(1), (int)reader.GetInt64(6));
        }

    }

    public class InputValue
    {
        public long Id { get; set; }
        public double Value { get; set; }
        public int StreamId { get; set; }
        public DateTime UTCFrom { get; set; }
        public DateTime UTCTo { get; set; }
        public bool IsDeletedValue { get; set; }
        public long ImportEventId { get; set; }
        public long ObsolescenceEventId { get; set; }
    }
}