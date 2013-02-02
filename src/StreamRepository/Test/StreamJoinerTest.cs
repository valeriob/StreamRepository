using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Diagnostics;
using StreamRepository;

namespace Test
{
    [TestClass]
    public class StreamJoinerTest
    {
        [TestMethod]
        public void when_joining_two_streams_the_whole_has_all_the_data()
        {
            var s1 = new MemoryStream();
            var s2 = new MemoryStream();
            s1.WriteByte(1);
            s2.WriteByte(2);

            var j = new StreamJoiner();
            j.Append(s1);
            j.Append(s2);

            var buffer = new byte[12];
            int read = j.Read(buffer, 0, 2);

            Assert.AreEqual(read, 2);
        }
    }

}
