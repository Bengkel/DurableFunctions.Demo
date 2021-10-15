using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ServerlessWorkshop
{
    [StorageAccount("AzureStorage_Connection")]
    public static class ImportChainFunction
    {
        [FunctionName("ImportChainFunction")]
        public static async Task<List<object>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            // Get filename from context
            var name = context.GetInput<string>();

            var outputs = new List<object>
            {
                await context.CallActivityAsync<int>("ImportChainFunction_Import", name),
                await context.CallActivityAsync<bool>("ImportChainFunction_Delete", name)
            };

            return outputs;
        }

        [FunctionName("ImportChainFunction_Import")]
        public static async Task<int> Import([ActivityTrigger] string name,
            [Blob("%input_blob_name%/{name}", FileAccess.Read)] Stream fileStream,
            [Table("addresses")] IAsyncCollector<Address> addresses,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            log.LogInformation($"{fileStream.Length} bytes.");

            int numberOfRecords = 0;

            using (StreamReader reader = new StreamReader(fileStream))
            {
                var columnNames = true;

                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();

                    if (columnNames)
                    {
                        columnNames = false;
                        continue;
                    }

                    numberOfRecords++;

                    string[] addressArray = line.Split(new char[] { ',' });
                    var address = new Address(addressArray);
                    await addresses.AddAsync(address);
                }
            }

            log.LogInformation
                (string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a filename in the URL."
                : $"Filename {name}, {numberOfRecords} records.");

            return numberOfRecords;
        }

        [FunctionName("ImportChainFunction_Delete")]
        public static async Task<bool> Delete([ActivityTrigger] string name,
             Binder binder,
             [Blob("%input_blob_name%/{name}", FileAccess.ReadWrite)] CloudBlob cloudBlob,
             ILogger log)
        {
            await cloudBlob.DeleteIfExistsAsync();

            log.LogInformation($"{name} is deleted");

            return true;
        }

        [FunctionName("ImportChainFunction_BlobStart")]
        public static async Task HttpStartAsync(
            [BlobTrigger("%input_blob_name%/{name}")] Stream myBlob,
            string name,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("ImportChainFunction", null, name);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            log.LogInformation(starter.CreateHttpManagementPayload(instanceId).StatusQueryGetUri);
        }
    }

    public class Address : TableEntity
    {
        public Address(string[] addressArray)
        {
            PartitionKey = addressArray[1];
            RowKey = addressArray[2];
            Province = addressArray[3];
            Municipality = addressArray[4];
            City = addressArray[5];
            Street = addressArray[6];
            StreetNumber = addressArray[7];
            StreetNumberAddition = addressArray[8];
            Lat = addressArray[9];
            Lng = addressArray[10];
        }

        public string Street { get; set; }

        public string StreetNumber { get; set; }

        public string StreetNumberAddition { get; set; }

        public string Postalcode { get { return PartitionKey; } set { PartitionKey = value; } }

        public string Kixcode { get { return RowKey; } set { RowKey = value; } }

        public string City { get; set; }

        public string Municipality { get; set; }

        public string Province { get; set; }

        public string Lat { get; set; }

        public string Lng { get; set; }
    }
}