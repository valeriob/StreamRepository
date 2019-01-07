using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.FileSystem
{
    public class FileLockException : Exception
    {
        public FileLockException(string message) : base(message)
        {

        }
    }

    public class FileLock : IDisposable
    {
        DirectoryInfo _dir;
        string _shardName;
        Guid _id;
        string _lockFile;

        public FileLock(DirectoryInfo dir, string shardName)
        {
            _dir = dir;
            _shardName = shardName;
            _id = Guid.NewGuid();
            _lockFile = Path.Combine(_dir.FullName, shardName + ".lock");

            LockShard();
        }

        public void Dispose()
        {
            UnlockShard();
        }

        void LockShard()
        {
            try
            {
                using (var file = File.Open(_lockFile, FileMode.CreateNew))
                using (var sw = new StreamWriter(file))
                {
                    sw.Write(_id.ToString());
                    sw.Flush();
                }
                return;
            }
            catch
            {

            }

            throw new FileLockException("Unable to lock");
        }

        void UnlockShard()
        {
            if (File.Exists(_lockFile))
            {
                var content = File.ReadAllText(_lockFile);
                if (content == _id.ToString())
                {
                    File.Delete(_lockFile);
                }
                else
                {
                    throw new Exception("Qualcosa è andato storto,  nn era il mio lock !");
                }
            }
            else
            {
                throw new Exception("Qualcosa è andato storto,  non era loccato !");
            }
        }

    }

    public class FileLockV2 : IDisposable
    {
        DirectoryInfo _dir;
        string _shardName;
        Guid _id;
        string _lockFile;
        FileStream _fileStream;

        public FileLockV2(DirectoryInfo dir, string shardName)
        {
            _dir = dir;
            _shardName = shardName;
            _id = Guid.NewGuid();
            _lockFile = Path.Combine(_dir.FullName, shardName + ".lock");
            try
            {
                _fileStream = File.Open(_lockFile, FileMode.OpenOrCreate);
            }
            catch
            {
                throw new FileLockException("Unable to lock");
            }
        }

        public void Dispose()
        {
            _fileStream.Dispose();
            try
            {
                File.Delete(_lockFile);
            }
            catch { }
        }

    }
}
