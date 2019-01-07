using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace StreamRepository
{
    public interface Repository<T> where T : ITimeValue
    {
        void AppendValues(T[] values);
        Task AppendValuesAsync(T[] allValues);

        IEnumerable<T> GetValues(DateTime? from = null, DateTime? to = null);

        void Compact();
        Task CompactAsync();
    }

    public interface ISerializeTimeValue<T> where T : ITimeValue
    {
        T Deserialize(byte[] item);

        T Deserialize(Stream stream);

        T[] DeserializeAll(Stream stream, long maxLength = long.MaxValue);

        void Serialize(T obj, Stream stream);

        byte[] Serialize(T obj);
    }

    public interface ITimeValue
    {
        DateTime Timestamp { get; }
        //string Id { get; set; }
    }
}
