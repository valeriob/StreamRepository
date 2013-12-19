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
        Task AppendValues(IEnumerable<ICanBeSharded> values);

        IEnumerable<object> GetValues(DateTime? from = null, DateTime? to = null);

        IEnumerable<byte[]> Get_Raw_Values(DateTime? from = null, DateTime? to = null);

        void Hint_Sampling_Period(int samplingPeriodInSeconds);

        //void Mark_Value_As_Obsolete(RecordValue value);

        void Reset();
    }


    public interface IBuildStuff
    {
        object Deserialize(BinaryReader reader);
        void Serialize(object obj, BinaryWriter writer);

        int SizeInBytes();
    }

    public interface ICanBeSharded
    {
        DateTime Timestamp { get; }
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
