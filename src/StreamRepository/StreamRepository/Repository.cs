using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public abstract class Repository
    {
        public async void Append_Value(DateTime timestamp, double value, int importId)
        {
            await Append_Values(new[] { new Tuple<DateTime, double, int>(timestamp, value, importId) });
        }

        public virtual async Task Append_Values(IEnumerable<Tuple<DateTime, double, int>> values)
        {
            foreach (var year in values.GroupBy(g => g.Item1.Year))
            {
                var header = Read_Header(year.Key);

                using (var stream = Open_Stream_For_Writing(year.Key))
                {
                    var tail = header.Index;

                    using (var buffer = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(buffer))
                            foreach (var value in year)
                            {
                                var framed = new FramedValue(value.Item1, value.Item2, value.Item3);
                                framed.Serialize(writer);
                            }

                        stream.Seek(tail, SeekOrigin.Begin);

                        int writtenBytes = year.Count() * FramedValue.SizeInBytes();
                        await stream.WriteAsync(buffer.GetBuffer(), 0, writtenBytes);

                        tail = tail + writtenBytes;
                    }

                    using (var buffer = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(buffer))
                        {
                            header.Index = tail;
                            header.Timestamp = DateTime.Now;
                            header.Serialize(writer);
                        }

                        stream.Seek(0, SeekOrigin.Begin);

                        await stream.WriteAsync(buffer.GetBuffer(), 0, StreamHeader.SizeInBytes());
                    }
                    stream.Flush();
                }
            }
        }


        public void Mark_Value_As_Obsolete(RecordValue value)
        {
            //var file = get value.Timestamp.Year
            // TODO fetch file
            using (var file = File.OpenWrite(""))
            {
                var obso = new FramedObsoleted(value.Position, true);
                var writer = new BinaryWriter(file);

                obso.Serialize(writer);
            }
        }


        public virtual IEnumerable<RecordValue> Get_Values()
        {
            foreach (var year in Get_Years())
            {
                using (var file = Open_Stream_For_Reading(year))
                {
                    // TODO read obsoleted log, and enrich value.

                    var reader = new BinaryReader(file);

                    var header = StreamHeader.Deserialize(reader);

                    file.Seek(StreamHeader.SizeInBytes(), SeekOrigin.Begin);
                    while (file.Position < header.Index)
                        yield return FramedValue.Deserialize(reader, file.Position);
                }
            }
        }
        public virtual IEnumerable<RecordValue> Get_Values(DateTime from)
        {
            foreach (var year in Get_Years().Where(y=> y >= from.Year))
            {
                using (var file = Open_Stream_For_Reading(year))
                {
                    // TODO read obsoleted log, and enrich value.

                    var reader = new BinaryReader(file);

                    var header = StreamHeader.Deserialize(reader);

                    file.Seek(StreamHeader.SizeInBytes(), SeekOrigin.Begin);
                    while (file.Position < header.Index)
                        //yield return FramedValue.Deserialize(reader, file.Position);
                        yield return FramedValue.Deserialize(file, file.Position);
                }
            }
        }


        public abstract void Hint_Year_Samples(int year, int samples);


        protected StreamHeader Read_Header(int year)
        {
            try
            {
                using (var stream = Open_Stream_For_Reading(year))
                {
                    var reader = new BinaryReader(stream);

                    return StreamHeader.Deserialize(reader);
                }
            }
            catch (System.IO.EndOfStreamException)
            {
                return new StreamHeader() { Index = StreamHeader.SizeInBytes() };
            }
        }

        protected abstract IEnumerable<int> Get_Years();
        protected abstract Stream Open_Stream_For_Reading(int year);
        protected abstract Stream Open_Stream_For_Writing(int year);
    }

    public class FramedValue
    {
        public DateTime Timestamp { get; private set; }
        public double Value { get; private set; }
        public int ImportId { get; private set; }


        public FramedValue(DateTime timestamp, double value, int importId)
        {
            Timestamp = timestamp.ToUniversalTime();
            Value = Value;
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

        public static RecordValue Deserialize(BinaryReader reader, long position)
        {
            byte[] buffer = new byte[20];
            reader.Read(buffer, 0, 20);
            return new RecordValue(buffer);

            //var dateTicks = reader.ReadInt64();
            //var date = DateTime.FromBinary(dateTicks);
            //var value = reader.ReadDouble();
            //var importId = reader.ReadInt32();
            //return new RecordValue(date, value, importId, position);
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

    public class StreamHeader
    {
        public int Index { get; set; }
        public DateTime Timestamp { get; set; }



        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Index);
            writer.Write(Timestamp.Ticks);
        }

        public static StreamHeader Deserialize(BinaryReader reader)
        {
            return new StreamHeader 
            {
                Index = reader.ReadInt32(),
                Timestamp = DateTime.FromBinary(reader.ReadInt64()),
            };
        }

        public static int SizeInBytes()
        {
            return 4+8;
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

}
