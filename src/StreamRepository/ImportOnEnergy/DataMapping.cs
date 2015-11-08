using StreamRepository;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportOnEnergy
{

    public static class DataReaderExtensions
    {
        public static InputValue ToInputValue(this IDataReader reader)
        {
            double value = 0;
            if (reader.IsDBNull(1) == false)
                value = reader.GetDouble(1);

            long obso = 0;
            if (reader.IsDBNull(3) == false)
                value = reader.GetInt64(3);

            return new InputValue
            {
                Id = reader.GetInt64(0),
                Value = value,
                StreamId = reader.GetInt32(2),
                ObsolescenceEventId = obso,
                UTCFrom = reader.GetDateTime(4),
                IsDeletedValue = reader.GetBoolean(5),
                ImportEventId = reader.GetInt64(6),
                UTCTo = reader.GetDateTime(7),
            };
        }

        public static Event ToEvent(this IDataReader reader)
        {
            double value = 0;
            if (reader.IsDBNull(1) == false)
                value = reader.GetDouble(1);

            return new Event(reader.GetDateTime(7), value, (int)reader.GetInt64(6));
        }
    }

    public class InputValueBuilder : ISerializeTimeValue<InputValue>
    {
        public TimeValue<InputValue> Deserialize(System.IO.BinaryReader reader)
        {
            var iv = new InputValue
            {
                Id = reader.ReadInt64(),
                Value = reader.ReadInt64(),
                UTCFrom = DateTime.FromBinary(reader.ReadInt64()),
                UTCTo = DateTime.FromBinary(reader.ReadInt64()),
                IsDeletedValue = reader.ReadBoolean(),
                ImportEventId = reader.ReadInt64(),
                ObsolescenceEventId = reader.ReadInt64(),
            };
            return new TimeValue<InputValue>(iv.UTCTo, iv);
        }

        public void Serialize(TimeValue<InputValue> iv, System.IO.BinaryWriter writer)
        {
            writer.Write(iv.Value.Id);
            writer.Write(iv.Value.Value);
            writer.Write(iv.Value.UTCFrom.ToBinary());
            writer.Write(iv.Value.UTCTo.ToBinary());
            writer.Write(iv.Value.IsDeletedValue);
            writer.Write(iv.Value.ImportEventId);
            writer.Write(iv.Value.ObsolescenceEventId);
        }

        public int SingleElementSizeInBytes()
        {
            return 8 + 8 + 8 + 8 + 1 + 8 + 8;
        }



        public object Deserialize2(System.IO.BinaryReader reader, int lenght)
        {
            var result = new InputValue[lenght];
            for (int i = 0; i < lenght; i++ )
            {
                result[i] = new InputValue
                {
                    Id = reader.ReadInt64(),
                    Value = reader.ReadInt64(),
                    UTCFrom = DateTime.FromBinary(reader.ReadInt64()),
                    UTCTo = DateTime.FromBinary(reader.ReadInt64()),
                    IsDeletedValue = reader.ReadBoolean(),
                    ImportEventId = reader.ReadInt64(),
                    ObsolescenceEventId = reader.ReadInt64(),
                };
            }
            return result;
        }

        public LazyTimeValue<InputValue> DeserializeLazy(byte[] raw)
        {
            throw new NotImplementedException();
        }
    }

    public struct InputValue
    {
        public long Id { get; set; }
        public double Value { get; set; }
        public int StreamId { get; set; }
        public DateTime UTCFrom { get; set; }
        public DateTime UTCTo { get; set; }
        public bool IsDeletedValue { get; set; }
        public long ImportEventId { get; set; }
        public long ObsolescenceEventId { get; set; }

        public DateTime Timestamp
        {
            get { return UTCTo; }
        }
    }
}
