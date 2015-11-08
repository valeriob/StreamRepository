using StreamRepository;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace ImportOnEnergy
{
    public class Importer
    {
        Account<InputValue> _account;
        string _cs;
        public int ImportedStreams { get; private set; }


        public Importer(Account<InputValue> account, string cs)
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
            var po = new ParallelOptions { MaxDegreeOfParallelism = 4 };
            //Parallel.ForEach(ids, po, id =>
            //{
            //    var sw = Stopwatch.StartNew();
            //    TryImportStream(id);
            //    sw.Stop();
            //    ImportedStreams++;

            //    Console.WriteLine(" Done in {0}", sw.Elapsed);
            //});
            foreach (var id in ids)
            {
                var sw = Stopwatch.StartNew();

                TryImportStream(id);
                sw.Stop();
                ImportedStreams++;

                Console.WriteLine(" Done in {0}", sw.Elapsed);
            }
        }
        void TryImportStream(int id)
        {
            Repository<InputValue> repository = null;
            var events = Enumerable.Empty<TimeValue<InputValue>>();
            while (true)
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
                catch (Exception ex)
                {
                    Console.WriteLine("Error reading Stram {0}  ", id);
                    Console.WriteLine("{0} - {1}", ex.Message, ex.StackTrace);
                }
            }

            while (true)
            {
                try
                {
                    repository = _account.BuildRepository(id + "");
                    repository.AppendValues(events.ToArray()).Wait();
                    break;
                }
                catch
                {
                    repository.Reset();
                }
            }
        }

        public IEnumerable<TimeValue<InputValue>> LoadEventsForStream(int id, IDbConnection con)
        {
            string query = @"SELECT Id, Value, StreamId, ObsolescenceEventId, UTCFrom, IsDeletedValue, ImportEventId, UTCTo from InputValue
                    WHERE StreamId = @StreamId";

            var parameters = new[] { Tuple.Create<string, object>("StreamId", id) };

            var result = new List<TimeValue<InputValue>>();
            using (var cmd = Prepare(con, query, parameters))
            using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                while (reader.Read())
                {
                    var ev = reader.ToInputValue();
                    result.Add(new TimeValue<InputValue>(ev.UTCTo, ev));
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

}
