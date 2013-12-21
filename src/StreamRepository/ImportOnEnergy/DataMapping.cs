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

    public class InputValueBuilder : IBuildStuff
    {
        public object Deserialize(System.IO.BinaryReader reader)
        {
            return new InputValue
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

        public void Serialize(object obj, System.IO.BinaryWriter writer)
        {
            var iv = (InputValue)obj;

            writer.Write(iv.Id);
            writer.Write(iv.Value);
            writer.Write(iv.UTCFrom.ToBinary());
            writer.Write(iv.UTCTo.ToBinary());
            writer.Write(iv.IsDeletedValue);
            writer.Write(iv.ImportEventId);
            writer.Write(iv.ObsolescenceEventId);
        }

        public int SingleElementSizeInBytes()
        {
            return 8 + 8 + 8 + 8 + 1 + 8 + 8;
        }

    }

    public class InputValue : ICanBeSharded
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
