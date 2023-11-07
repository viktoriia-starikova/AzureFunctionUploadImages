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
        private readonly string STORAGE_CONNECTION_STRING = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        private readonly string STORAGE_CONTAINER_NAME = Environment.GetEnvironmentVariable("ContainerName");
        private readonly string COSMOS_DB_ENDPOINT = Environment.GetEnvironmentVariable("CosmosDBEndpoint");
        private readonly string COSMOS_DB_KEY = Environment.GetEnvironmentVariable("CosmosDBKey");
        private readonly ILogger<PostImage> _logger;

        // Cosmos DB client instance
        private CosmosClient _cosmosClient;
        private Database _database;
        private Container _container;

        // Blob storage
        private const string DATABASE_ID = "Images";
        private const string CONTAINER_ID = "TaskState";

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
                IFormFile file = req.Form.Files["File"];
                using Stream fileStream = file.OpenReadStream();

                BlobClient blobClient = GetBlobClient(file.FileName);

                // Upload an image
                await blobClient.UploadAsync(fileStream, overwrite: true);
                var url = blobClient.Uri.ToString();

                // Cosmos DB
                using CosmosClient cosmosClient = await SetUpCosmosClient();
                var result = await AddTaskStateToContainerAsync(blobClient.Name, url, string.Empty, "Created");

                // Service Bus Topic
                await SendMessageToServiceBusTopic(result.TaskId);

                return result.TaskId;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message, ex);
                throw;
            }
        }

        [FunctionName("GetTaskState")]
        public async Task<string> GetTaskState(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            try
            {
                using CosmosClient cosmosClient = await SetUpCosmosClient();
                Console.WriteLine(cosmosClient == _cosmosClient);
                string id = req.GetQueryParameterDictionary()["id"];
                var result = await GetCosmosDBItemAsync(id);
                if (result.ProcessedFilePath.Length > 0)
                    return result.ProcessedFilePath;

                return result.State;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message, ex);
                throw;
            }
        }

        private async Task<CosmosClient> SetUpCosmosClient()
        {
            _cosmosClient = new CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY, new CosmosClientOptions() { ApplicationName = "CosmosDB" });

            await CreateDatabaseAsync();
            await CreateContainerAsync();

            return _cosmosClient;
        }

        private async Task<TaskState> GetCosmosDBItemAsync(string id)
        {
            var sqlQueryText = $"SELECT * FROM c WHERE c.id = '{id}'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<TaskState> queryResultSetIterator = _container.GetItemQueryIterator<TaskState>(queryDefinition);

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
            string busConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsServiceBusTopic");
            string topicName = Environment.GetEnvironmentVariable("TopicName");

            var message = new Message(Encoding.UTF8.GetBytes(id));

            var topicClient = new TopicClient(busConnectionString, topicName);

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

            await _container.CreateItemAsync<TaskState>(task);
            return await _container.ReadItemAsync<TaskState>(task.TaskId, new PartitionKey(task.TaskId));
        }

        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            _database = await _cosmosClient.CreateDatabaseIfNotExistsAsync(DATABASE_ID);
            Console.WriteLine("Created Database: {0}\n", _database.Id);
        }

        private async Task CreateContainerAsync()
        {
            // Create a new container
            _container = await _database.CreateContainerIfNotExistsAsync(CONTAINER_ID, "/id");
            Console.WriteLine("Created Container: {0}\n", _container.Id);
        }

        private BlobClient GetBlobClient(string fileName)
        {
            // Blob storage
            BlobContainerClient blobContainer = new BlobContainerClient(STORAGE_CONNECTION_STRING, STORAGE_CONTAINER_NAME);
            BlobClient blobClient = blobContainer.GetBlobClient(fileName);

            return blobClient;
        }
    }
}

