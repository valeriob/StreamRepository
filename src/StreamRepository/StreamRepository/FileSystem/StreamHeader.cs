using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public class StreamHeader
    {
        public int Index { get; set; }
        public DateTime Timestamp { get; set; }
        public Guid ShardingStrategy { get; set; }

        public DateTime LastEventTimestamp { get; set; }
        public double LastEventValue { get; set; }


        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Index);
            writer.Write(Timestamp.ToBinary());
            writer.Write(ShardingStrategy.ToByteArray());

            writer.Write(LastEventTimestamp.ToBinary());
            writer.Write(LastEventValue);
        }

        public static StreamHeader Deserialize(BinaryReader reader)
        {
            var header = new StreamHeader
            {
                Index = reader.ReadInt32(),
                Timestamp = DateTime.FromBinary(reader.ReadInt64()),
                ShardingStrategy = new Guid(reader.ReadBytes(16)),
            };

            try
            {
                header.LastEventTimestamp = DateTime.FromBinary(reader.ReadInt64());
                header.LastEventValue = reader.ReadDouble();

            }
            catch { }

            return header;
        }

        public static int SizeInBytes()
        {
            return 4 + 8 + 16 + 8 + 8;
        }

        internal void Update(IBuildStuff _builder, ICanBeSharded lastEvent, int index)
        {
            Index = index;
            Timestamp = DateTime.Now;

            if (lastEvent != null)
            {
                LastEventTimestamp = lastEvent.Timestamp;
                LastEventValue = lastEvent.Value;
            }
        }

    }

}
