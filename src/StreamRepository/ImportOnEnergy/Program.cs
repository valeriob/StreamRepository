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

            //TestImportedData(account);
            RunImport(account);
        }
        public static void TestImportedData(Account account)
        {
            account.Read_Streams();
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

    public class Importer
    {
        Account _account;
        string _cs;
        public int ImportedStreams { get; private set; }


        public Importer(Account account, string cs)
        {
            _account = account;
            _cs = cs;
        }


        public void ImportAllStreams()
        {
            var ids = new List<int>();
            using (var con = new System.Data.SqlClient.SqlConnection(_cs))
            {
                con.Open();
                ids = GetStreamIds(con).OrderBy(d => d).ToList();
            }
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

                TryImportStream(id);
                sw.Stop();
                ImportedStreams++;

                //Console.WriteLine("Imported stream {0} in {1}", id, sw.Elapsed);

                Console.WriteLine(" Done in {0}", sw.Elapsed);
            }
        }
        void TryImportStream(int id)
        {
            Repository repository = null;
            var events = Enumerable.Empty<Event>();
            while(true)
            {
                try
                {
                    using (var con = new System.Data.SqlClient.SqlConnection(_cs))
                    {
                        con.Open();
                        events = LoadEventsForStream(id, con);
                        Console.Write("Stram {0} read {1} elements  ", id, events.Count());
                    }
                    break;
                }
                catch(Exception ex)
                {
                    Console.WriteLine("Error reading Stram {0}  ", id);
                    Console.WriteLine("{0} - {1}", ex.Message, ex.StackTrace);
                    //Console.WriteLine("Retri")
                }
            }

            while (true)
            {
                try
                {
                    repository = _account.BuildRepository(id + "");
                    repository.AppendValues(events).Wait();
                   // Console.WriteLine(" Done", id);
                    break;
                }
                catch
                {
                    repository.Reset();
                }
            }
        }

        public IEnumerable<Event> LoadEventsForStream(int id, IDbConnection con)
        {
            string query = @"SELECT Id, Value, StreamId, ObsolescenceEventId, UTCFrom, IsDeletedValue, ImportEventId, UTCTo from InputValue
                    WHERE StreamId = @StreamId";

            var parameters = new[] { Tuple.Create<string, object>("StreamId", id) };

            var result = new List<Event>();
            using (var cmd = Prepare(con, query, parameters))
            using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                {
                    var ev = reader.ToEvent();
                    result.Add(ev);
                }
            return result;
        }

        IEnumerable<int> GetStreamIds(IDbConnection connection)
        {
            string query = "select Id from Stream where IsObsolete = 0";

            var ids = new List<int>();

            using (var cmd = Prepare(connection, query))
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
            double value = 0;
            if (reader.IsDBNull(1) == false)
                value = reader.GetDouble(1);

            return new Event(reader.GetDateTime(7), value, (int)reader.GetInt64(6));
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
