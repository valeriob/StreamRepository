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
        Task AppendValues(ICanBeSharded[] values);

        IEnumerable<object> GetValues(DateTime? from = null, DateTime? to = null);

        IEnumerable<byte[]> GetRawValues(DateTime? from = null, DateTime? to = null);

        void HintSamplingPeriod(int samplingPeriodInSeconds);

        void Reset();
    }


    public interface IBuildStuff
    {
        object Deserialize(BinaryReader reader);
        void Serialize(object obj, BinaryWriter writer);

        int SingleElementSizeInBytes();

        object Deserialize2(BinaryReader reader, int lenght);
    }

    public interface ICanBeSharded
    {
        DateTime Timestamp { get; }
        double Value { get; }
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
