# Parallel file processing using Azure Durable Functions

## Problem statement

Azure durable functions is an extension of Azure Functions and allows you to build long running stateful workflows in a serverless environment. In this example we will take input files from `raw` folder and process these files in parallel in `4` steps. Each step will be executed in a separate function and output is saved in respective folders.

## Why use Serverless

Main advantages of using serverless technologies are:

- Pay only for what you use
- No need to manage infrastructure and deployment
- Code first and suitable to wait for external changes and events
- Quickly Scalable for spiky workloads

## Getting started

### Running locally

1. Clone this repository
2. Create `local.setting.json` file with following content

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "Replace with your storage account connection string to be used by Azure durable functions for state management",
    "DataStorage": "Replace with storage account connection string with raw data",
    "DataContainer": "Replace with storage account container name",
    "Step1DataSubpath": "folder name for step 1 output",
    "Step2DataSubpath": "folder name for step 2 output",
    "Step3DataSubpath": "folder name for step 3 output",
    "Step4DataSubpath": "folder name for step 4 output",
    "FUNCTIONS_WORKER_RUNTIME": "python"
  }
}
```

3. use vscode and follow steps to run this function [run locally](https://docs.microsoft.com/en-us/azure/azure-functions/create-first-function-vs-code-python#run-the-function-locally)

4. Trigger execution of durable function using curl where
   1. `rawDataPath` is folder name to generate random files
   2. `numFiles` number of random file to generate
   3. `numRows` number of rows to generate in each file

```bash
 curl --location --request POST 'http://localhost:7071/api/orchestrators/OrchestratorFunc' \
--header 'Content-Type: application/json' \
--data-raw '{
    "rawDataPath": "raw",
    "numFiles": "4",
    "numRows": "10"
}'
```

sample response

```json
{
  "id": "2647a2542e104c0fbb6a69e6cf360d51",
  "statusQueryGetUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2647a2542e104c0fbb6a69e6cf360d51?taskHub=TestHubName&connection=Storage&code=zernwZj2pkCdkWkerVCu7M1IeCMQejyobHLMbH2dTZxLq4Twy1Koyw==",
  "sendEventPostUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2647a2542e104c0fbb6a69e6cf360d51/raiseEvent/{eventName}?taskHub=TestHubName&connection=Storage&code=zernwZj2pkCdkWkerVCu7M1IeCMQejyobHLMbH2dTZxLq4Twy1Koyw==",
  "terminatePostUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2647a2542e104c0fbb6a69e6cf360d51/terminate?reason={text}&taskHub=TestHubName&connection=Storage&code=zernwZj2pkCdkWkerVCu7M1IeCMQejyobHLMbH2dTZxLq4Twy1Koyw==",
  "rewindPostUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2647a2542e104c0fbb6a69e6cf360d51/rewind?reason={text}&taskHub=TestHubName&connection=Storage&code=zernwZj2pkCdkWkerVCu7M1IeCMQejyobHLMbH2dTZxLq4Twy1Koyw==",
  "purgeHistoryDeleteUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2647a2542e104c0fbb6a69e6cf360d51?taskHub=TestHubName&connection=Storage&code=zernwZj2pkCdkWkerVCu7M1IeCMQejyobHLMbH2dTZxLq4Twy1Koyw==",
  "restartPostUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2647a2542e104c0fbb6a69e6cf360d51/restart?taskHub=TestHubName&connection=Storage&code=zernwZj2pkCdkWkerVCu7M1IeCMQejyobHLMbH2dTZxLq4Twy1Koyw=="
}
```

you can query the status of the function using the status query uri `statusQueryGetUri`.

5. Deploy this function using anyone of the methods. [Link](https://docs.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies)

## References

- <https://docs.microsoft.com/en-us/azure/azure-functions/durable/quickstart-python-vscode>
- <https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-sequence?tabs=python>
- <https://github.com/Azure/azure-functions-durable-python/tree/dev/samples>
- <https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-sub-orchestrations?tabs=python>

## License

MIT

---

> GitHub [@lordlinus](https://github.com/lordlinus) &nbsp;&middot;&nbsp;
> Twitter [@lordlinus](https://twitter.com/lordlinus) &nbsp;&middot;&nbsp;
> Linkedin [Sunil Sattiraju](https://www.linkedin.com/in/sunilsattiraju/)
