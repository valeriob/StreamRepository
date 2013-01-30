using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public static class SystemExtensions
    {
        public static T GetAttribute<T>(this Type type) where T : Attribute
        {
            return (T)Attribute.GetCustomAttribute(type, typeof(T));
        }
    }
}
