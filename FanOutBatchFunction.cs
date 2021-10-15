using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace ServerlessWorkshop
{
    public static class FanOutBatchFunction
    {
        [FunctionName("FanOutDemoFunction")]
        public static async Task<bool[]> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,

            ILogger log)
        {
            try
            {
                // Get context
                var jsonContent = context.GetInput<string>();
                dynamic items = JsonConvert.DeserializeObject(jsonContent);

                var startTime = DateTime.Now;

                //EXPLAIN The requested number of files
                int numberOfFiles = int.Parse(items["numberOfFiles"].Value);

                //EXPLAIN The files per batch
                int batchNumber = int.Parse(items["batchNumber"].Value);

                //Explain Calculate number of tasks
                int numberOfTasks = numberOfFiles / batchNumber;

                // Declare Task Array
                var writeTasks = new Task<bool>[numberOfTasks];

                //EXPLAIN F1 FAN-OUT
                for (int n = 0; n < numberOfTasks; n++)
                {
                    writeTasks[n] = context.CallActivityAsync<bool>(
                                            "FanOutDemoFunction_Write",
                                            new int[] { n, batchNumber });
                }

                log.LogInformation($"Created {numberOfTasks} parallel tasks.");

                // Create container to store files
                CloudStorageAccount account = CloudStorageAccount.Parse("{your-storage-account-connection-string}");
                CloudBlobClient serviceClient = account.CreateCloudBlobClient();

                var container = serviceClient.GetContainerReference("fanoutdemo");
                container.CreateIfNotExistsAsync().Wait();

                //EXPLAIN F3 FAN-IN
                var insertResults = await Task.WhenAll(writeTasks);

                log.LogInformation($"Ended in {DateTime.Now.Subtract(startTime).TotalMinutes} minutes.");

                return insertResults;
            }
            catch (Exception exception)
            {
                log.LogError(exception.Message);
                return new bool[] { false };
            }
        }

        //EXPLAIN F2 PARALLEL FUNCTIONS
        [FunctionName("FanOutDemoFunction_Write")]
        public static bool Write([ActivityTrigger] int[] index, ILogger log)
        {
            var success = false;

            var maximum = (index[0] + 1) * index[1];

            // Create container to store files
            CloudStorageAccount account = CloudStorageAccount.Parse("{your-storage-account-connection-string}");
            CloudBlobClient serviceClient = account.CreateCloudBlobClient();

            var container = serviceClient.GetContainerReference("fanoutdemo");

            BlobRequestOptions blobRequestOptions = new BlobRequestOptions
            {
                ParallelOperationThreadCount = 64,

                //RetryPolicy = new RetryPolicies.LinearRetry(TimeSpan.FromSeconds(5), 4)
            };

            for (int n = maximum - index[1]; n < maximum; n++)
            {
                //EXPLAIN Create file name
                var name = $"file-{n}.txt";

                log.LogInformation($"Write file {name}");

                try
                {
                    //EXPLAIN Write file to storage
                    CloudBlockBlob blob = container.GetBlockBlobReference(name);
                    blob.UploadTextAsync($"Hello, this is  {name}.", null, null, blobRequestOptions, null).Wait();
                    success = true;
                }
                catch (Exception exception)
                {
                    log.LogError(exception.Message);
                }
            }

            return success;
        }

        //F1 TRIGGER FUNCTIONS
        [FunctionName("FanOutDemoFunction_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Get request content.
            var content = req.Content;
            string jsonContent = content.ReadAsStringAsync().Result;

            //EXPLAIN Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("FanOutDemoFunction", Guid.NewGuid().ToString(), jsonContent);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}
