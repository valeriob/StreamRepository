using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public interface IShard<T>  where T : ITimeValue
    {
        void AppendValues(ISerializeTimeValue<T> builder, IEnumerable<T> shardValues);
        Task AppendValuesAsync(ISerializeTimeValue<T> builder, IEnumerable<T> shardValues);
        IEnumerable<T> FetchValues(ISerializeTimeValue<T> builder);
        IEnumerable<T[]> FetchValuesBatch(ISerializeTimeValue<T> builder);
        void Compact(ISerializeTimeValue<T> builder);
    }
}