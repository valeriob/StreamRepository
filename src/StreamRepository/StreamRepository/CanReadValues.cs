using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public interface CanReadValues
    {
        IEnumerable<RecordValue> Read_Values();
    }
}
