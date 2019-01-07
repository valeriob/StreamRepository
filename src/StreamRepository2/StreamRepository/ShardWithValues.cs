
using System.Collections.Generic;

namespace StreamRepository
{
    public interface ShardWithValues<T> where T : ITimeValue
    {
        string GetName();
        IEnumerable<T> GetValues();
    }

}
