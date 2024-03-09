using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;

namespace DurableFunctionApp.Orchestrators
{
    public class DocumentRequestData
    {
        public string? InstanceId { get; set; }
        public string? DocumentUrl { get; set; }
        public string? DocumentText { get; set; }
    }

    public class DocumentResponseData
    {
        public string? Entities { get; set; }
        public string? Summary { get; set; }
        public string? DocumentUrl { get; set; }

        public string? KeyPhrases { get; set; }

    }

    public class DocumentProcessingOrchestrator
    {
        private readonly ILogger<DocumentProcessingOrchestrator> _logger;
        public DocumentProcessingOrchestrator(ILogger<DocumentProcessingOrchestrator> logger)
        {
            this._logger = logger;
        }

        [Function(nameof(StartDocumentProcessingOrchestrator))]
        public async Task<HttpResponseData> StartDocumentProcessingOrchestrator([HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req,
            [DurableClient] DurableTaskClient durableTaskClient)
        {
            var documentDto = await req.ReadFromJsonAsync<DocumentRequestData>();
            if (documentDto == null || string.IsNullOrEmpty(documentDto.DocumentUrl))
            {
                return req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            }
            else
            {
                documentDto.InstanceId = documentDto.InstanceId ?? Guid.NewGuid().ToString();
            }

            string instanceId = await durableTaskClient.ScheduleNewOrchestrationInstanceAsync(nameof(RunDocumentProcessingOrchestrator), input:documentDto, new StartOrchestrationOptions(documentDto.InstanceId));
            _logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return durableTaskClient.CreateCheckStatusResponse(req, instanceId);
        }



        [Function(nameof(RunDocumentProcessingOrchestrator))]
        public async Task<DocumentResponseData> RunDocumentProcessingOrchestrator([OrchestrationTrigger] TaskOrchestrationContext context, [DurableClient] DurableTaskClient durableTaskClient)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(RunDocumentProcessingOrchestrator));
            logger.LogInformation($"Starting {nameof(RunDocumentProcessingOrchestrator)}.");
            var documentDto = context.GetInput<DocumentRequestData>();            

            // First get the document text.
            var documentText = await context.CallActivityAsync<string>(
                nameof(ExtractDocumentText),
                documentDto);

            documentDto.DocumentText = documentText;
            // Use Fan out and Fan in pattern to run multiple task parallely
            var parallelTasks = new List<Task<string>>();

            // Fan out
            var detectEntitiesTaskResult = context.CallActivityAsync<string>(
                nameof(DetectEntitiesFromDocumentText),
                documentDto);
            parallelTasks.Add(detectEntitiesTaskResult);

            var keyPhrasesTaskResult = context.CallActivityAsync<string>(
                nameof(ExtractKeyPhrasesFromDocumentText),
                documentDto);
            parallelTasks.Add(keyPhrasesTaskResult);

            var summaryTaskResult = context.CallActivityAsync<string>(
                nameof(CreateSummaryOfDocumentText),
                documentDto);     
            
            parallelTasks.Add(summaryTaskResult);

            //Fan in:  Wait for all tasks to be completed.
            await Task.WhenAll(parallelTasks);

            return new DocumentResponseData
            {                
                Entities = detectEntitiesTaskResult.Result,
                KeyPhrases = keyPhrasesTaskResult.Result,
                Summary = summaryTaskResult.Result
            };            
        }


        [Function(nameof(ExtractDocumentText))]
        public async Task<string> ExtractDocumentText([ActivityTrigger] DocumentRequestData documentRequestData,
            [DurableClient] DurableTaskClient durableTaskClient)
        {
            _logger.LogInformation($"Running {nameof(ExtractDocumentText)} for document url --------------- {documentRequestData.DocumentUrl}");
            if (documentRequestData?.InstanceId != null)
            {
                var instance = await durableTaskClient.GetInstanceAsync(documentRequestData.InstanceId);
                if (instance != null && instance.RuntimeStatus != OrchestrationRuntimeStatus.Terminated && instance.RuntimeStatus != OrchestrationRuntimeStatus.Failed)
                {
                    // Sleeping for 30 seconds to mimic the actual processing time to extract the text from document.
                    Thread.Sleep(30000);
                    _logger.LogInformation($"Extracted Text for document url --------------- {documentRequestData.DocumentUrl}");
                    return $"Extarcted Document Text";
                }
            }

            return $"Terminated for document url --------------- {documentRequestData?.DocumentUrl}";
        }

        
        [Function(nameof(CreateSummaryOfDocumentText))]
        public async Task<string> CreateSummaryOfDocumentText([ActivityTrigger] DocumentRequestData documentRequestData,
            [DurableClient] DurableTaskClient durableTaskClient)
        {
            _logger.LogInformation($"Running {nameof(CreateSummaryOfDocumentText)} for document url --------------- {documentRequestData.DocumentUrl}");
            if (documentRequestData?.InstanceId != null)
            {
                var instance = await durableTaskClient.GetInstanceAsync(documentRequestData.InstanceId);
                if (instance != null && instance.RuntimeStatus != OrchestrationRuntimeStatus.Terminated && instance.RuntimeStatus != OrchestrationRuntimeStatus.Failed)
                {
                    // Sleeping for 30 seconds to mimic the actual processing time to create summary of the text from document.
                    // TODO: Write business logic here to create document summary.
                    Thread.Sleep(30000);
                    _logger.LogInformation($"Completed Summary of Text for document url --------------- {documentRequestData.DocumentUrl}");
                    return $"Summary of Document";
                }
            }

            return $"Terminated for document url --------------- {documentRequestData?.DocumentUrl}";
        }

        [Function(nameof(DetectEntitiesFromDocumentText))]
        public async Task<string> DetectEntitiesFromDocumentText([ActivityTrigger] DocumentRequestData documentRequestData,
            [DurableClient] DurableTaskClient durableTaskClient)
        {
            _logger.LogInformation($"Running {nameof(DetectEntitiesFromDocumentText)} for document url --------------- {documentRequestData.DocumentUrl}");
            if (documentRequestData?.InstanceId != null)
            {
                var instance = await durableTaskClient.GetInstanceAsync(documentRequestData.InstanceId);
                if (instance != null && instance.RuntimeStatus != OrchestrationRuntimeStatus.Terminated && instance.RuntimeStatus != OrchestrationRuntimeStatus.Failed)
                {
                    // Sleeping for 30 seconds to mimic the actual processing time to detect entities from document.
                    // TODO: Write business logic here to detect entities.
                    Thread.Sleep(30000);
                    _logger.LogInformation($"Completed detect entities for document url --------------- {documentRequestData.DocumentUrl}");
                    return $"PERSON: Huzefa ; Location: New York";
                }
            }

            return $"Terminated for document url --------------- {documentRequestData?.DocumentUrl}";
        }

        [Function(nameof(ExtractKeyPhrasesFromDocumentText))]
        public async Task<string> ExtractKeyPhrasesFromDocumentText([ActivityTrigger] DocumentRequestData documentRequestData,
            [DurableClient] DurableTaskClient durableTaskClient)
        {
            _logger.LogInformation($"Running {nameof(ExtractKeyPhrasesFromDocumentText)} for document url --------------- {documentRequestData.DocumentUrl}");
            if (documentRequestData?.InstanceId != null)
            {
                var instance = await durableTaskClient.GetInstanceAsync(documentRequestData.InstanceId);
                if (instance != null && instance.RuntimeStatus != OrchestrationRuntimeStatus.Terminated && instance.RuntimeStatus != OrchestrationRuntimeStatus.Failed)
                {
                    // Sleeping for 30 seconds to mimic the actual processing time to Extract Key Phrases From Document Text.
                    // TODO: Write business logic here to Extract Key Phrases.
                    Thread.Sleep(30000);
                    _logger.LogInformation($"Completed Extract Key Phrases for document url --------------- {documentRequestData.DocumentUrl}");
                    return $"This is an agreement document between Huzefa and Other Party.";
                }
            }

            return $"Terminated for document url --------------- {documentRequestData?.DocumentUrl}";
        }
    }
}
