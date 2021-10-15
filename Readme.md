* Usage

Create a local.settings.json file and add the following properties.

>{
>   "IsEncrypted": false,
>  "Values": {
>    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
>    "FUNCTIONS_WORKER_RUNTIME": "dotnet",
>    "AzureStorage_Connection": "{your storage account connection string}",
>    "input_blob_name": "import",
>    "input_table_name": "addresses"
>  }
>}

Create a storage account and add the connection string to the settings.

Drop the CSV in the storage cointainer "import" to test the import function.

Post to json below to test the FanOutBatch function.

>{
>    "numberOfFiles" : "100000",
>    "batchNumber": "100"
>}