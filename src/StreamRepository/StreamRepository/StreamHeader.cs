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


        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Index);
            writer.Write(Timestamp.Ticks);
            writer.Write(ShardingStrategy.ToByteArray());
        }

        public static StreamHeader Deserialize(BinaryReader reader)
        {
            return new StreamHeader
            {
                Index = reader.ReadInt32(),
                Timestamp = DateTime.FromBinary(reader.ReadInt64()),
                ShardingStrategy = new Guid(reader.ReadBytes(16)),
            };
        }

        public static int SizeInBytes()
        {
            return 4 + 8;
        }
    }
}
