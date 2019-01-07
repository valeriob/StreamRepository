using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MessagePack;
using StreamRepository;
using StreamRepository.FileSystem;

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
            var filePath = @"e:\temp\Rtls";
            var ff = new FileSystemFactory<RtlsEvent>(new FileSystemShardingStrategy<RtlsEvent>[] { new FileSystemPerYearShardingStrategy<RtlsEvent>(),
                new FileSystemPerMonthShardingStrategy<RtlsEvent>() }, new RtlsEventBuilder());
            Account<RtlsEvent> account = new FileSystemAccount<RtlsEvent>(filePath, ff, new FileSystemPerYearShardingStrategy<RtlsEvent>());


            var tb = new RtlsEventTestBuilder();
            //watch = Stopwatch.StartNew();
            //account.Reset();
            //account.Write_Streams(1, 1, 10, tb.Build);
            //watch.Stop();
            //Console.WriteLine(watch.Elapsed);

            watch = Stopwatch.StartNew();
            account.Read_Streams();
            watch.Stop();
            Console.WriteLine(watch.Elapsed);

            Console.WriteLine("hit enter to loop reading");
            Console.ReadLine();


            var streams = account.GetStreams().ToArray();
            var random = new Random();
            var id = random.Next(streams.Length);
            var stream = streams[id];
            
            var rep = account.BuildRepository(stream);
            rep.Compact();
            while (true)
            {


                watch = Stopwatch.StartNew();
                var count = rep.GetValues(new DateTime(2012, 1, 1)).Count();
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

    [MessagePack.MessagePackObject]
    public class RtlsEvent : ITimeValue
    {
        [MessagePack.Key(0)]
        public DateTime Timestamp { get; set; }
        [MessagePack.Key(1)]
        public string CarrelloId { get; set; }
        [MessagePack.Key(2)]
        public byte TipoAttivita { get; set; }

        [MessagePack.Key(3)]
        public decimal? PosizioneX { get; set; }
        [MessagePack.Key(4)]
        public decimal? PosizioneY { get; set; }
        [MessagePack.Key(5)]
        public decimal? AltezzaForche { get; set; }
        [MessagePack.Key(6)]
        public decimal? DistanzaForche { get; set; }

        [MessagePack.Key(7)]
        public bool? InMovimento { get; set; }
        [MessagePack.Key(8)]
        public bool? Carico { get; set; }
        [MessagePack.Key(9)]
        public decimal MetriPercorsi { get; set; }

        [MessagePack.Key(10)]
        public int MissioniEseguite { get; set; }

        [MessagePack.Key(11)]
        public int MovimentiDiPresaInCarico { get; set; }
        [MessagePack.Key(12)]
        public int PedanePreseInCarico { get; set; }

    }

    public class RtlsEventBuilder : ISerializeTimeValue<RtlsEvent>
    {
        public RtlsEvent Deserialize(byte[] bytes)
        {
            throw new NotImplementedException();
            //return MessagePack.MessagePackSerializer.Deserialize<RtlsEvent>(bytes);
        }

        public RtlsEvent Deserialize(Stream stream)
        {
            return MessagePack.MessagePackSerializer.Deserialize<RtlsEvent>(stream, true);
        }

        public RtlsEvent[] DeserializeAll(Stream stream, long maxLength = long.MaxValue)
        {
            var result = new List<RtlsEvent>();
            
            while (true)
            {
                RtlsEvent value = null;
                try
                {
                    value = Deserialize(stream);
                    result.Add(value);
                }
                catch (InvalidOperationException)
                {
                    break;
                }
            }

            return result.ToArray();
        }

        public void Serialize(RtlsEvent obj, Stream stream)
        {
            MessagePack.MessagePackSerializer.Serialize(stream,obj);
            //MessagePack.MessagePackSerializer.Serialize(stream, obj);
        }

        public byte[] Serialize(RtlsEvent obj)
        {
            throw new NotImplementedException();
        }

    }
    public class RtlsEventTestBuilder
    {
        Random _random = new Random(DateTime.Now.Millisecond);
        string carrelloId = "carrello/1";

        public RtlsEvent Build(DateTime timestamp)
        {
            return new RtlsEvent
            {
                CarrelloId = carrelloId,
                Timestamp = timestamp,
                TipoAttivita = 1,

                PosizioneX = (decimal)_random.Next(100),
                PosizioneY = (decimal)_random.Next(100),
                AltezzaForche = (decimal)_random.NextDouble() * 10,
                DistanzaForche = (decimal)_random.NextDouble() * 10,

                InMovimento = _random.Next(2) == 0,
                Carico = _random.Next(2) == 0,
                MetriPercorsi = (decimal)_random.NextDouble(),

                MovimentiDiPresaInCarico = _random.Next(3),
                PedanePreseInCarico = _random.Next(3),
                MissioniEseguite = _random.Next(3),

            };
        }
    }

    public class UnboundedArray<T>
    {

    }

    //public class mptf<T> : MessagePack.Formatters.IMessagePackFormatter<UnboundedArray<T>>
    //{
    //    public UnboundedArray<T> Deserialize(byte[] bytes, int offset, IFormatterResolver formatterResolver, out int readSize)
    //    {
    //        var f = formatterResolver.GetFormatter<T>();
            
    //        f.Deserialize(bytes, offset, formatterResolver, )
    //    }

    //    public int Serialize(ref byte[] bytes, int offset, UnboundedArray<T> value, IFormatterResolver formatterResolver)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
}
