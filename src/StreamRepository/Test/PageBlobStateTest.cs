using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using StreamRepository.Azure;
using System.IO;

namespace Test
{
    [TestClass]
    public class PageBlobStateTest
    {
        [TestMethod]
        public void when_writting_to_a_pageBlob_all_data_is_correctly_retrieved()
        {
            var azureAccount = new CloudStorageAccount(new StorageCredentials("valeriob", "2SzgTAaG11U0M1gQ19SNus/vv1f0efwYOwZHL1w9YhTKEYsU1ul+s/ke92DOE1wIeCKYz5CuaowtDceUvZW2Rw=="), true);
            var blobClient = azureAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("test");
            container.CreateIfNotExists();

            var blob = container.GetPageBlobReference(Guid.NewGuid()+"");

            var helper = new PageBlobState(blob);
            //helper.Create_if_does_not_exists();
            helper.Open();

            var output = "";
            var input = @"The uniqueifier is NULL for the first instance of each customer_id, and is then populated, in ascending order, for each subsequent row with the same customer_id value. The overhead for rows with a NULL uniqueifier value is, unsurprisingly, zero bytes. This is why min_record_size_in_bytes remained unchanged in the overhead table; the first insert had a uniqueifier value of NULL. This is also why it is impossible to estimate how much additional storage overhead will result from the addition of a uniqueifier, without first having a thorough understanding of the data being stored. For example, a non-unique clustered index on a datetime column may have very little overhead if data is inserted, say, once per minute. However, if that same table is receiving thousands of inserts per minute, then it is likely that many rows will share the same datetime value, and so the uniqueifier will have a much higher overhead.

If your requirements seem to dictate the use of a non-unique clustered key, my advice would be to look to see if there are a couple of relatively narrow columns that, together, can form a unique key. You'll still see the increase in the row size for your clustering key in the index pages of both your clustered and nonclustered indexes, but you'll at least save the cost of the uniqueifier in the data pages of the leaf level of your clustered index. Also, instead of storing an arbitrary uniqueifier value to the index key, which is meaningless in the context of your data, you would be adding meaningful and potentially useful information to all of your nonclustered indexes.

A good clustered index is also built upon static, or unchanging, columns. That is, you want to choose a clustering key that will never be updated. SQL Server must ensure that data exists in a logical order based upon the clustering key. Therefore, when the clustering key value is updated, the data may need to be moved elsewhere in the clustered index so that the clustering order is maintained. Consider a table with a clustered index on LastName, and two non-clustered indexes, where the last name of an employee must be updated.";

            helper.Append(input);


            using (var stream = helper.OpenReadonlyStream())
            {
                using (var reader = new StreamReader(stream))
                {
                    output = reader.ReadToEnd();
                }
            }

            Assert.AreEqual(input, output);
        }
    }
}
