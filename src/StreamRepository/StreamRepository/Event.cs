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
        public ITimeValue<Event> Deserialize(BinaryReader reader)
        {
            var timestamp = DateTime.FromBinary(reader.ReadInt64());
            var value = reader.ReadDouble();
            var importId = reader.ReadInt32();

            return new Event(timestamp, value, importId);
        }

        public LazyTimeValue<Event> DeserializeLazy(byte[] raw)
        {
            var timestamp = DateTime.FromBinary(BitConverter.ToInt64(raw, 0));
            return new LazyTimeValue<Event>(timestamp, raw, this);
        }


        public void Serialize(ITimeValue<Event> evnt, BinaryWriter writer)
        {
            writer.Write(evnt.Timestamp.ToBinary());
            writer.Write(evnt.Payload.Value);
            writer.Write(evnt.Payload.ImportId);
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

    public struct Event : ITimeValue<Event>
    {
        public DateTime Timestamp { get; private set; }

        public Event Payload
        {
            get
            {
                return this;
            }
        }

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
