using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using sogoapi.data.Models;
using sogoapi.web.data.Models;

namespace Sogo.TransactionQueueSb;

public static class HandleTransactionQueueItems
{
    private static string _endpointUrl;
    private static string _primaryKey;
    private static string _databaseId = "sogodb";
    private static string _containerId = "sogocollection";
    private static CosmosClient _cosmosClient;

    private static List<Fee> _fees = new List<Fee>();

    [FunctionName("HandleTransactionQueueItems")]
    public static async Task RunAsync([ServiceBusTrigger("transaction-queue-sb", Connection = "SB_CONNECTION")] string queueItem, ILogger log)
    {
       // log.LogInformation($"C# ServiceBus queue trigger function processed message: {queueItem}");
        
        _endpointUrl = GetEnvironmentVariable("COSMOS_ENDPOINT_URL");
        _primaryKey = GetEnvironmentVariable("PRIMARY_KEY");
        _databaseId = GetEnvironmentVariable("DATABASE_ID");
        _containerId = GetEnvironmentVariable("CONTAINER_ID");

        _cosmosClient = new CosmosClient(_endpointUrl, _primaryKey,
            new CosmosClientOptions() {AllowBulkExecution = true, ConnectionMode = ConnectionMode.Gateway});

        TransactionQueueObject transactionQueueObject = JsonConvert.DeserializeObject<TransactionQueueObject>(queueItem);

        if (transactionQueueObject.TaskType == "calc_round_fee" && transactionQueueObject.Round != null)
        {
            bool isGuid = false;
            if (Guid.TryParse(transactionQueueObject.Round.TransactionId, out var newGuid))
                isGuid = true;
            else
                log.LogCritical($"transaction id is not a valid UUID ! {transactionQueueObject.Round.TransactionId}");
            
            //does this round already have a transactionId ? If it does its likely already been charged
            if (isGuid 
                && transactionQueueObject.Round.TransactionId != null 
                && transactionQueueObject.Round.EntityId == "adceb3ea-52b8-4fa9-8279-633beca45417" //<== MSL 
                && transactionQueueObject.Round.OriginalSource != "admin_panel")
            {
                //the source of this request was <**NOT from admin_panel**>, eg. was mobile_app
                //since there is ALREADY a transaction for this same MSL round id, we will now go
                //and get that transaction and check if its a "debit" for a round. if it is, then we
                //will do NOTHING, otherwise we will end up charging golfer more than once
                log.LogInformation($"<< MSL >> round w/ id {transactionQueueObject.Round.Id} already has a SOGO transaction associated with it");
                Transaction transaction = await GetTransaction(transactionQueueObject.Round.TransactionId, log);

                if (transaction != null && transaction.TransactionType.DebitOrCredit == "debit") /* there's already a debit transaction for this round */
                {
                    log.LogInformation($"debit transaction already exists for this MSL round. {transaction.TransactionValue} tokens");
                } else if (transaction != null)
                {
                    //its some other sort of transaction, maybe a credit which would be highly unusual if its
                    //coming via the mobile app
                }
            }
            else if ((transactionQueueObject.Round.TransactionId == null || transactionQueueObject.Round.TransactionId == string.Empty)
                     && transactionQueueObject.Round.EntityId == "adceb3ea-52b8-4fa9-8279-633beca45417" //<== MSL 
                     && transactionQueueObject.Round.OriginalSource != "admin_panel")
            {
                await CreateAndSaveTransaction(transactionQueueObject, transactionQueueObject.TokenCost, log);
            }
        }
    }

    private static async Task<Transaction> GetTransaction(string roundTransactionId, ILogger log)
    {
        string query = $"select * from c where c.id = '{roundTransactionId}'";
        QueryDefinition queryDefinition = new QueryDefinition(query);
        FeedIterator<Transaction> queryResultSetIterator = _cosmosClient.GetContainer(_databaseId, _containerId).GetItemQueryIterator<Transaction>(queryDefinition);

        while (queryResultSetIterator.HasMoreResults)
        {
            var currentResultSet = await queryResultSetIterator.ReadNextAsync();

            foreach (Transaction transaction in currentResultSet)
            {
                return transaction;
            }
        }
        return null;
    }

    private static async Task<float> CalculateTokenCost(TransactionQueueObject transactionQueueObject, ILogger log)
    {
        //what fee will we use for this round?
        var numHoles = transactionQueueObject.Round.HoleScores.Count;
        var feeIdentifier = numHoles == 18 ? "cost_18Holes" : "cost_9Holes";
        var entityId = transactionQueueObject.Round.EntityId;
        Fee fee = _fees.First(fee => fee.EntityId == entityId && fee.Item == feeIdentifier);
        
        return fee.Cost;
    }

    private static async Task GetFeeCostSchedule()
    {
        string query = "SELECT * from c where c.type = 'fee'";
        QueryDefinition queryDefinition = new QueryDefinition(query);
        FeedIterator<Fee> queryResultSetIterator = _cosmosClient.GetContainer(_databaseId, _containerId).GetItemQueryIterator<Fee>(queryDefinition);

        while (queryResultSetIterator.HasMoreResults)
        {
            var currentResultSet = await queryResultSetIterator.ReadNextAsync();

            foreach (Fee fee in currentResultSet)
            {
                _fees.Add(fee);
            }
        }
    }

    private static async Task<Transaction> CreateAndSaveTransaction(TransactionQueueObject transactionQueueObject, int costOfRound, ILogger log)
    {
        var currentBalance = await GetCurrentBalance(transactionQueueObject, log);

        if (currentBalance != null)
        {
            var newTransaction = new Transaction
            {
                Id = Guid.NewGuid().ToString(),
                Type = "transaction",
                EntityId = transactionQueueObject.EntityId,
                Email = transactionQueueObject.GolferEmail,
                AvailableTokens = (currentBalance - costOfRound) ?? 0,
                GolferId = transactionQueueObject.GolferId,
                GolferFirstName = transactionQueueObject.GolferFirstName,
                GolferLastName = transactionQueueObject.GolferFirstName,
                GolferEmail = transactionQueueObject.GolferEmail,
                TransactionDate = DateTime.Now,
                TransactionValue = Convert.ToInt32(costOfRound),
                TransactionType = new TransactionType
                {
                    Name = "token_purchase",
                    EntityId = transactionQueueObject.EntityId,
                    ShortDescription = "Tokens Debit (new round)",
                    Type = "debit",
                    DebitOrCredit = "debit",
                    CreatedDate = DateTime.UtcNow,
                    UpdateDate = null,
                    UpdateUserId = null
                },
                ThirdPartyRoundId = transactionQueueObject.Round.ThirdPartyScorecardId,
                TransactionNotes = "new round",
                OriginalSource = transactionQueueObject.Round.OriginalSource,
                UpdateSource = null,
                CreatedDate = DateTime.UtcNow,
                UpdateDate = null,
                UpdateUserId = null
            };
        
            //try to create a new transaction oncosmosdb
            try
            {
                await _cosmosClient.GetContainer(_databaseId, _containerId).UpsertItemAsync(newTransaction, new PartitionKey("transaction"));
                log.LogInformation($"<< MSL >> round we CAN charge a fee. cost: {costOfRound} tokens :: roundid: {transactionQueueObject.Round.Id} {transactionQueueObject.Round.GolferEmail}");

                return newTransaction;
            }
            catch (Exception e)
            {
                log.LogError(e.Message, e);
                throw;
            }
        }
        else
        {
            return null;
        }
    }
    
    private static async Task<int?> GetCurrentBalance(TransactionQueueObject transactionQueueObject, ILogger logger)
    {
        string query = $"SELECT TOP 1 c.availableTokens from c where c.type = 'transaction' and c.golferId = '{transactionQueueObject.GolferId}' order by c.transactionDate desc";
        QueryDefinition queryDefinition = new QueryDefinition(query);
        FeedIterator<Transaction> queryResultSetIterator = _cosmosClient.GetContainer(_databaseId, _containerId).GetItemQueryIterator<Transaction>(queryDefinition);

        while (queryResultSetIterator.HasMoreResults)
        {
            var currentResultSet = await queryResultSetIterator.ReadNextAsync();
            return currentResultSet.First().AvailableTokens;
        }

        return null;
    }

    
    private static string GetEnvironmentVariable(string name)
    {
        return Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
    }

    
    public class TransactionQueueObject
    {
        public string TaskType { get; set; }
        public int TokenCost { get; set; }
        public string EntityId { get; set; }
        public string GolferId { get; set; }
        public string GolferEmail { get; set; }
        public string GolferFirstName { get; set; }
        public string GolferLastName { get; set; }

        public Round Round { get; set; }
    }
}