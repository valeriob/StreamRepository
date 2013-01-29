using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{

    public class FileStream_With_Hard_Flush : FileStream
    {
        public FileStream_With_Hard_Flush(string path, FileMode fileMode)
            : base(path, fileMode)
        { }

        public override void Flush()
        {
            base.Flush(true);
            // FlushFileBuffers(base.SafeFileHandle.DangerousGetHandle());
        }

        [DllImport("kernel32.dll")]
        static extern bool FlushFileBuffers(IntPtr hFile);
    }
}
