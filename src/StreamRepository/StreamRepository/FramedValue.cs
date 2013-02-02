using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public class FramedValue
    {
        public DateTime Timestamp { get; private set; }
        public double Value { get; private set; }
        public int ImportId { get; private set; }


        public FramedValue(DateTime timestamp, double value, int importId)
        {
            Timestamp = timestamp.ToUniversalTime();
            Value = value;
            ImportId = importId;
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Timestamp.Ticks);
            writer.Write(Value);
            writer.Write(ImportId);
        }

        public static FramedValue Deserialize(BinaryReader reader)
        {
            return new FramedValue(DateTime.FromBinary(reader.ReadInt64()), reader.ReadDouble(), reader.ReadInt32());
        }

        public static RecordValue Deserialize(Stream stream, long position)
        {
            byte[] buffer = new byte[20];
            stream.Read(buffer, 0, 20);
            return new RecordValue(buffer);
        }

        public static int SizeInBytes()
        {
            return 8 + 8 + 4;
        }
    }

}
