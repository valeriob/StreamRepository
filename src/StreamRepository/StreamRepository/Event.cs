using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public class EventBuilder : ISerializeTimeValue<Event>
    {
        public TimeValue<Event> Deserialize(BinaryReader reader)
        {
            var timestamp = DateTime.FromBinary(reader.ReadInt64());
            var value = reader.ReadDouble();
            var importId = reader.ReadInt32();

            var evnt = new Event(timestamp, value, importId);
            return new TimeValue<Event>(timestamp, evnt);
        }

        public LazyTimeValue<Event> DeserializeLazy(byte[] raw)
        {
            var timestamp = DateTime.FromBinary(BitConverter.ToInt64(raw, 0));
            return new LazyTimeValue<Event>(timestamp, raw, this);
        }


        public void Serialize(TimeValue<Event> evnt, BinaryWriter writer)
        {
            writer.Write(evnt.Value.Timestamp.ToBinary());
            writer.Write(evnt.Value.Value);
            writer.Write(evnt.Value.ImportId);
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

    public class Event
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
