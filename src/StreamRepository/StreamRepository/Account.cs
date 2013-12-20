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

            var opt = new ParallelOptions { MaxDegreeOfParallelism = 4 };

            foreach (var stream in GetStreams())
            {
                streams++;
                var repository = BuildRepository(stream);
                foreach (var value in repository.GetValues())
                    values++;

                var speed = values / watch.Elapsed.TotalSeconds;
                Console.WriteLine("Completed {0} number {1} : {2:0} total of {3} ", stream, streams, speed, values);
            }

            watch.Stop();

            Console.WriteLine("read {0} values in {1} streams in {2}", values, streams, watch.Elapsed);
        }

        public void Write_Streams(int streams, int years, int samplingPeriodInSeconds)
        {
            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = 3
            };
            try
            {
                for (int i = 0; i < streams; i++)
                    Write_Stream(Guid.NewGuid() + "", years, samplingPeriodInSeconds);
            }
            catch (AggregateException ex)
            {
                foreach (var exception in ex.InnerExceptions)
                    Console.WriteLine(exception.Message);
                throw;
            }
            //Parallel.For(0, streams, options, i =>
            //{
            //    Write_Stream(Guid.NewGuid() + "", years, samplingPeriodInSeconds);
            //    Console.WriteLine("Working on {0}° stream", i);
            //});
        }

        public void Write_Stream(string name, int years, int samplingPeriodInSeconds)
        {
            var repository = BuildRepository(name);

            for (int year = DateTime.Now.Year - years; year < DateTime.Now.Year; year++)
                Write_Year(repository, year, samplingPeriodInSeconds);
        }

        public void Write_Year(Repository repository, int year, int samplingPeriodInSeconds)
        {
            var random = new Random();
            var since = new DateTime(year, 1, 1);
            var watch = Stopwatch.StartNew();
            int batchSize = 10000;

            int samples = (365 * 24 * 60 * 60) / samplingPeriodInSeconds;
            batchSize = int.MaxValue;
            var batch = new List<Event>();

            repository.HintSamplingPeriod( samples);
            for (int i = 1; i < samples + 1; i++)
            {
                batch.Add(new Event(since.AddSeconds(samplingPeriodInSeconds), random.NextDouble(), (i / batchSize) + 1));

                if (i % batchSize == 0 && i != 1)
                {
                    repository.AppendValues(batch).Wait();
                    batch.Clear();

                    var remaining = TimeSpan.FromTicks((watch.Elapsed.Ticks / i) * (samples - i));
                    //Console.WriteLine("{0} / {1},  {2:0} %    remaining : {3}", i, samples, ((double)i / samples) * 100, remaining);
                }
            }
            repository.AppendValues(batch).Wait();

            watch.Stop();

            //Console.WriteLine("Written {2} : Elapses {0}, append/s {1}", watch.Elapsed, 525600 / watch.Elapsed.TotalSeconds, year);
        }

        

        public abstract void Reset();
        public abstract Repository BuildRepository(string streamName);
        public abstract IEnumerable<string> GetStreams();
    }

}
