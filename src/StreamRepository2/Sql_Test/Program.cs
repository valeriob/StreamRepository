using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sql_test
{
    class Program
    {
        static void Main(string[] args)
        {
            var con = new SqlConnection(@"Data Source=(localdb)\Projects;Initial Catalog=Test;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False");
            con.Open();

            //using (var cmd = con.CreateCommand())
            //{
            //    cmd.CommandText = "TRUNCATE TABLE Test.dbo.Table_Insert";
            //    cmd.ExecuteNonQuery();
            //}

            //int count = 365 * 24 * 60 * 5;
            //var watch = Stopwatch.StartNew();
            //Insert(con, count, 1024, new DateTime(2008, 1, 1));
            //watch.Stop();
            //Console.WriteLine(watch.Elapsed);

            Query(con, new DateTime(2010,1,1));// new DateTime(2012, 1, 1));

            Console.ReadLine();
        }

        public static void Insert(DbConnection con, int count, int batch, DateTime since)
        {
            var random = new Random();

            for (int i = 0; i < count; i += batch)
            {
                var trans = con.BeginTransaction();
                for (int j = i; j < batch + i; j += 1)
                {
                    using (var cmd = con.CreateCommand())
                    {
                        cmd.Transaction = trans;
                        cmd.CommandText = @"INSERT INTO dbo.TABLE_Insert VALUES(@timestamp, @value, @importId)";
                        var par = cmd.CreateParameter();
                        //par.Value = j;
                        //par.ParameterName = "id";
                        //cmd.Parameters.Add(par);

                        par = cmd.CreateParameter();
                        par.Value = since.AddMinutes(j);
                        par.ParameterName = "timestamp";
                        cmd.Parameters.Add(par);

                        par = cmd.CreateParameter();
                        par.Value = random.NextDouble();
                        par.ParameterName = "value";
                        cmd.Parameters.Add(par);

                        par = cmd.CreateParameter();
                        par.Value = random.Next();
                        par.ParameterName = "importId";
                        cmd.Parameters.Add(par);

                        cmd.ExecuteNonQuery();
                    }
                }
                
                trans.Commit();
            }
        }

        public static void Query(DbConnection con, DateTime from)
        {
            int count = 0;
            var watch = Stopwatch.StartNew();

            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM dbo.TABLE_Insert where timestamp > @from";

                var par = cmd.CreateParameter();
                par.Value = from;
                par.ParameterName = "from";
                cmd.Parameters.Add(par);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetInt32(0);
                        var timestamp = reader.GetDateTime(1);
                        var value = reader.GetDouble(2);
                        var importId = reader.GetInt32(3);
                        count++;
                    }

                }
            }
            watch.Stop();
            Console.WriteLine(watch.Elapsed);
        }
    }

    public class StressTestContext : DbContext 
    {
        public IDbSet<InputValue> Values { get; set; }
    }


    public class InputValue
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
        public double Value { get; set; }
        public int ImportId { get; set; }
    }

}
