using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public interface Repository<T>
    {
        Task AppendValues(TimeValue<T>[] values);

        IEnumerable<TimeValue<T>> GetValues(DateTime? from = null, DateTime? to = null);

        IEnumerable<byte[]> GetRawValues(DateTime? from = null, DateTime? to = null);

        void HintSamplingPeriod(int samplingPeriodInSeconds);

        void Reset();
    }


    public interface ISerializeTimeValue<T>
    {
        TimeValue<T> Deserialize(BinaryReader reader);
        void Serialize(TimeValue<T> obj, BinaryWriter writer);

        int SingleElementSizeInBytes();

        //object Deserialize2(BinaryReader reader, int lenght);
    }

    public class TimeValue<T>
    {
        public DateTime Timestamp { get; private set; }
        public T Value { get; private set; }

        public TimeValue(DateTime timestamp, T value)
        {
            Timestamp = timestamp;
            Value = value;
        }
    }

    public class LazyTimeValue<T>
    {
        public DateTime Timestamp { get; private set; }

        byte[] _payload;
        T _value;
        ISerializeTimeValue<T> _serializer;
        public T GetValue()
        {
            if(_value == null)
            {
                _value = _serializer.Deserialize(new BinaryReader(new MemoryStream(_payload))).Value;
            }
            return _value;
        }

        public LazyTimeValue(DateTime timestamp, byte[] payload, ISerializeTimeValue<T> serializer)
        {
            Timestamp = timestamp;
            _payload = payload;
            _serializer = serializer;
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
