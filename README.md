# AzureFunctionUploadImages
- Frontend part - only swagger (no UI needed). On the server side, we use Azure API Manager.
    1. Endpoint to send the images for processing:
    - Gets the file
    - Returns task **ID**
    - Saves file in Azure Blob Storage
    - Saves task state in Azure Cosmos DB. The state should include next information:
        1. File name
        2. Original file path in Azure Blob Storage
        3. The processed file path in Azure Blob Storage
        4. Task ID
        5. State (Done / In progress / Created / Error)
    - Put the task in the Azure Service Bus Topics
    1. Endpoint to get the state of the task using Task ID / Link to the processed image file, if the file is ready
