using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public interface Repository
    {
        Task AppendValues(IEnumerable<Event> values);

        IEnumerable<RecordValue> Get_Values(DateTime? from = null, DateTime? to = null);

        IEnumerable<byte[]> Get_Raw_Values(DateTime? from = null, DateTime? to = null);

        void Hint_Sampling_Period(int samplingPeriodInSeconds);

        //void Mark_Value_As_Obsolete(RecordValue value);
    }

    public class Event
    {
        //public int Id { get; set; }
        public DateTime Timestamp { get; private set; }
        public double Value { get; private set; }
        public int ImportId { get; private set; }

        public Event(DateTime timestamp, double value, int importId)
        {
            Timestamp = timestamp;
            Value = value;
            ImportId = importId;
        }
    }

 


    public class FramedObsoleted 
    {
        public long Position { get; private set; }
        public bool Obsoleted { get; private set; }

        public FramedObsoleted(long position, bool obsoleted)
        {
            Position = position;
            Obsoleted = obsoleted;
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Position);
            writer.Write(Obsoleted);
        }

        public static FramedObsoleted Deserialize(BinaryReader reader)
        {
            return new FramedObsoleted(reader.ReadInt64(), reader.ReadBoolean());
        }

        public static int SizeInBytes()
        {
            return 8 + 4;
        }
    }


}
