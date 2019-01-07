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
        public long Index { get; set; }
        public DateTime Timestamp { get; set; }
        public long NumberOfEvents { get; set; }

        public DateTime FirstEventTimestamp { get; set; }
        public DateTime LastEventTimestamp { get; set; }

        public StreamHeader(DateTime firstEventTimestamp)
        {
            FirstEventTimestamp = firstEventTimestamp;
            LastEventTimestamp = firstEventTimestamp;
        }

        public StreamHeader()
        {

        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Index);
            writer.Write(Timestamp.ToBinary());
            writer.Write(FirstEventTimestamp.ToBinary());
            writer.Write(LastEventTimestamp.ToBinary());
            writer.Write(NumberOfEvents);
        }

        public static StreamHeader Deserialize(BinaryReader reader)
        {
            var header = new StreamHeader
            {
                Index = reader.ReadInt64(),
                Timestamp = DateTime.FromBinary(reader.ReadInt64()),
                FirstEventTimestamp = DateTime.FromBinary(reader.ReadInt64()),
                LastEventTimestamp = DateTime.FromBinary(reader.ReadInt64()),
                NumberOfEvents = reader.ReadInt64()
            };

            return header;
        }

        public void UpdateTimestamps(DateTime timestamp)
        {
            if (FirstEventTimestamp == DateTime.MinValue)
            {
                FirstEventTimestamp = timestamp;
            }
            else
            {
                if (timestamp <= FirstEventTimestamp)
                    FirstEventTimestamp = timestamp;
            }

            if(LastEventTimestamp == DateTime.MinValue)
            {
                LastEventTimestamp = timestamp;
            }
            else
            {
                if (timestamp > LastEventTimestamp)
                    LastEventTimestamp = timestamp;
            }
        }

        public void Update(long index, int addedEvents)
        {
            Index = index;
            NumberOfEvents = addedEvents;
            Timestamp = DateTime.Now;
        }

    }

}
