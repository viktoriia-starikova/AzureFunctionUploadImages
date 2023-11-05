using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using AzureFunctionUploadImages2;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.ServiceBus;
using System.Text;

namespace ImageUploadAPI
{
    public class PostImage
    {
        private readonly ILogger<PostImage> _logger;

        private Container container;

        // Cosmos DB client instance
        private CosmosClient cosmosClient;

        // Cosmos DB
        private Database database;

        private readonly string databaseId = "Images";
        private readonly string containerId = "TaskState";

        public PostImage(ILogger<PostImage> log)
        {
            _logger = log;
        }

        [FunctionName("PostImage")]
        public async Task<string> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req)
        {
            try
            {
                Stream fileStream = new MemoryStream();
                IFormFile file = req.Form.Files["File"];
                fileStream = file.OpenReadStream();

                BlobClient blobClient = GetBlobClient(file.FileName);

                // Upload an image
                await blobClient.UploadAsync(fileStream, overwrite: true);
                var url = blobClient.Uri.ToString();

                // Cosmos DB
                SetUpCosmosClient();
                await CreateDatabaseAsync();
                await CreateContainerAsync();
                var result = await AddTaskStateToContainerAsync(blobClient.Name, url, string.Empty, "created");

                // Service Bus Topic
                await SendMessageToServiceBusTopic(result.TaskId);

                return result.TaskId;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [FunctionName("GetTaskState")]
        public async Task<string> GetTaskState(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            try
            {
                SetUpCosmosClient();
                await CreateDatabaseAsync();
                await CreateContainerAsync();

                string id = req.GetQueryParameterDictionary()["id"];
                var result = await GetCosmosDBItemAsync(id);
                if (result.ProcessedFilePath.Length > 0)
                    return result.ProcessedFilePath;

                return result.State;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private void SetUpCosmosClient()
        {
            string CosmosDBEndpoint = Environment.GetEnvironmentVariable("CosmosDBEndpoint");
            string CosmosDBKey = Environment.GetEnvironmentVariable("CosmosDBKey");

            this.cosmosClient = new CosmosClient(CosmosDBEndpoint, CosmosDBKey, new CosmosClientOptions() { ApplicationName = "CosmosDB" });
        }

        private async Task<TaskState> GetCosmosDBItemAsync(string id)
        {
            var sqlQueryText = $"SELECT * FROM c WHERE c.id = '{id}'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<TaskState> queryResultSetIterator = this.container.GetItemQueryIterator<TaskState>(queryDefinition);

            List<TaskState> tasks = new List<TaskState>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<TaskState> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (TaskState task in currentResultSet)
                {
                    tasks.Add(task);
                    Console.WriteLine("\tRead {0}\n", task);
                }
            }
            return tasks.First();
        }

        static async Task SendMessageToServiceBusTopic(string id)
        {
            string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsServiceBusTopic");
            string topicName = Environment.GetEnvironmentVariable("TopicName");

            var message = new Message(Encoding.UTF8.GetBytes(id));

            var topicClient = new TopicClient(connectionString, topicName);

            await topicClient.SendAsync(message);
        }

        private async Task<TaskState> AddTaskStateToContainerAsync(string name, string path, string processedPath, string state)
        {
            TaskState task = new TaskState
            {
                FileName = name,
                OriginalFilePath = path,
                ProcessedFilePath = processedPath,
                State = state,
                TaskId = Guid.NewGuid().ToString()
            };

            await this.container.CreateItemAsync<TaskState>(task);
            return await this.container.ReadItemAsync<TaskState>(task.TaskId, new PartitionKey(task.TaskId));
        }

        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Created Database: {0}\n", this.database.Id);
        }

        private async Task CreateContainerAsync()
        {
            // Create a new container
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, "/id");
            Console.WriteLine("Created Container: {0}\n", this.container.Id);
        }

        private BlobClient GetBlobClient(string fileName)
        {
            // Blob storage
            string connection = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string containerName = Environment.GetEnvironmentVariable("ContainerName");

            BlobContainerClient blobContainer = new BlobContainerClient(connection, containerName);
            BlobClient blobClient = blobContainer.GetBlobClient(fileName);

            return blobClient;
        }
    }
}

