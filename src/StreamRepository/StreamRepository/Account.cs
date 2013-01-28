using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public abstract class Account
    {
        public void Read_Streams()
        {
            long values = 0;
            int streams = 0;
            var watch = Stopwatch.StartNew();

            foreach (var stream in Get_Streams().Take(2))
            {
                streams++;
                var repository = Build_Repository(stream);
                foreach (var value in repository.Get_Values())
                    values++;

                var speed = values / watch.Elapsed.TotalSeconds;
                Console.WriteLine("Completed  number {1} : {2:0} total of {3} ", stream, streams, speed, values / 1000000);
            }
            watch.Stop();

            Console.WriteLine("read {0} values in {1} streams in {2}", values, streams, watch.Elapsed);
        }

        public void Write_Streams(int streams, int years)
        {
            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = 3
            };

            Parallel.For(0, streams, options, i =>
            {
                Write_Stream(Guid.NewGuid() + "", years).Wait();
                Console.WriteLine("Working on {0}° stream", i);
            });
        }

        public async Task Write_Stream(string name, int years)
        {
            var repository = Build_Repository(name);

            for (int year = DateTime.Now.Year - years; year < DateTime.Now.Year; year++)
                await Write_Year(year, repository);
        }

        public async Task Write_Year(int year, Repository repository)
        {
            var random = new Random();
            var since = new DateTime(year, 1, 1);
            var watch = Stopwatch.StartNew();
            int batchSize = 10000;

            //int samples = 365 * 24 * 60; // 525600, minutes 
            int samples = 365 * 24 * 4; // 525600, quaters of an hour 
            var batch = new List<Tuple<DateTime, double, int>>();

            repository.Hint_Year_Samples(since.Year, samples);
            for (int i = 1; i < samples + 1; i++)
            {
                //repository.Append_Value(since.AddMinutes(i), random.NextDouble(), (i / 100) + 1);
                batch.Add(new Tuple<DateTime, double, int>(since.AddMinutes(i * 15 /* *15*/), random.NextDouble(), (i / batchSize) + 1));

                if (i % batchSize == 0 && i != 1)
                {
                    await repository.Append_Values(batch);
                    batch.Clear();

                    var remaining = TimeSpan.FromTicks((watch.Elapsed.Ticks / i) * (samples - i));
                    //Console.WriteLine("{0} / {1},  {2:0} %    remaining : {3}", i, samples, ((double)i / samples) * 100, remaining);
                }
            }
            await repository.Append_Values(batch);

            watch.Stop();

            //Console.WriteLine("Written {2} : Elapses {0}, append/s {1}", watch.Elapsed, 525600 / watch.Elapsed.TotalSeconds, year);
        }


        public abstract void Reset();
        public abstract Repository Build_Repository(string streamName);
        public abstract IEnumerable<string> Get_Streams();
    }

}
