using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository
{
    public static class NamingUtilities
    {
        public static string Get_Index_File(DirectoryInfo directory)
        {
            return Path.Combine(directory.FullName, "index.dat");
        }

        public static string Get_Index_File(CloudBlobDirectory directory)
        {
            return new Uri(directory.Uri, "index.dat").ToString();
        }

        //public static string Get_Factory_Blob(CloudBlobDirectory directory, string factoryId)
        //{
        //    return new Uri(directory.Uri, "factory-" + factoryId).ToString();
        //}
    }
}
