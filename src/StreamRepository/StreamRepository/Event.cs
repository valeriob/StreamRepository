using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public class EventBuilder : IBuildStuff
    {
        public object Deserialize(BinaryReader reader)
        {
            var timestamp = DateTime.FromBinary(reader.ReadInt64());
            var value = reader.ReadDouble();
            var importId = reader.ReadInt32();

            return new Event(timestamp, value, importId);
        }

        public void Serialize(object obj, BinaryWriter writer)
        {
            var evnt = (Event)obj;
            writer.Write(evnt.Timestamp.ToBinary());
            writer.Write(evnt.Value);
            writer.Write(evnt.ImportId);
        }

        public int SingleElementSizeInBytes()
        {
            return 8 + 8 + 4;
        }



        public object Deserialize2(BinaryReader reader, int lenght)
        {
            throw new NotImplementedException();
        }
    }

    public class Event : ITimeValue
    {
        public DateTime Timestamp { get; private set; }
        public double Value { get; private set; }
        public int ImportId { get; private set; }

        public Event(DateTime timestamp, double value, int importId) //:this()
        {
            Timestamp = timestamp;
            Value = value;
            ImportId = importId;
        }

    }
}
