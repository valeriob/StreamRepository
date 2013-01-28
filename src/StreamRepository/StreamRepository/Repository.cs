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
        public void Append_Value(DateTime timestamp, double value, int importId)
        {
            Append_Values(new[] { new Tuple<DateTime, double, int>(timestamp, value, importId) });
        }


        public abstract void Append_Values(IEnumerable<Tuple<DateTime, double, int>> values);

        public abstract IEnumerable<RecordValue> Get_Values(DateTime? from = null, DateTime? to = null);

        public abstract IEnumerable<byte[]> Get_Raw_Values(DateTime? from = null, DateTime? to = null);

        public abstract void Hint_Year_Samples(int year, int samples);


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
