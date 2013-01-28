using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public class RecordValue
    {
        DateTime? _timestamp;
        public DateTime Timestamp
        {
            get
            {
                if (_timestamp == null)
                    _timestamp = DateTime.FromBinary(BitConverter.ToInt64(_buffer, 0));
                return _timestamp.Value;
            }
        }
        double? _value;
        public double Value
        {
            get
            {
                if (_value == null)
                    _value = BitConverter.ToDouble(_buffer, 0);
                return _value.Value;
            }
        }
        int? _importId;
        public int ImportId
        {
            get
            {
                if (_importId == null)
                    _importId = BitConverter.ToInt32(_buffer, 0);
                return _importId.Value;
            }
        }
        long? _position;
        public long Position
        {
            get
            {
                if (_position == null)
                    _position = BitConverter.ToInt64(_buffer, 0);
                return _importId.Value;
            }
        }

        byte[] _buffer;
        public RecordValue(byte[] buffer)
        {
            _buffer = buffer;
        }

    }


    //public class RecordValue
    //{
    //    public DateTime Timestamp { get; private set; }
    //    public double Value { get; private set; }
    //    public int ImportId { get; private set; }

    //    public long Position { get; private set; }

    //    public RecordValue(DateTime timestamp, double value, int importId, long position)
    //    {
    //        Timestamp = timestamp;
    //        Value = Value;
    //        ImportId = importId;

    //        Position = position;
    //    }

    //}

}
