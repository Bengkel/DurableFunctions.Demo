**Usage**

1. Create a local.settings.json file and add the following properties.

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

2. Create a storage account and add the connection string to the settings.

3. Drop the CSV in the storage cointainer "import" to test the import function.

4. Post to json below to test the FanOutBatch function.

>{
>    "numberOfFiles" : "100000",
>    "batchNumber": "100"
>}
