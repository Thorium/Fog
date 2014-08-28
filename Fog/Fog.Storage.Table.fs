module Fog.Storage.Table

open Microsoft.WindowsAzure.Storage.Table
open Fog.Core

let BuildTableClientWithConnStr(connectionString) =
    memoize (fun conn -> 
                  let storageAccount = GetStorageAccount conn
                  storageAccount.CreateCloudTableClient()
            ) connectionString 

let BuildTableClient() = BuildTableClientWithConnStr "TableStorageConnectionString"


let private doTableAction (client:CloudTableClient) (tableName:string) (operation:TableOperation) =
    let table = client.GetTableReference(tableName)
    table.ExecuteAsync(operation) |> Async.AwaitTask

let private doTableActionWithCreate (client:CloudTableClient) (tableName:string) (operation:TableOperation) =
    async {
        let table = client.GetTableReference(tableName)
        let! created = table.CreateIfNotExistsAsync() |> Async.AwaitTask
        return! table.ExecuteAsync(operation) |> Async.AwaitTask
    }

let CreateEntityWithClient client tableName (entity: 'a) = 
    let dynEntity = entity |> box :?> TableEntity
    doTableActionWithCreate client tableName (TableOperation.Insert dynEntity)

let DeleteEntityWithDataContext client tableName (entity: 'a) =
    let dynEntity = entity |> box :?> TableEntity
    doTableAction client tableName (TableOperation.Delete dynEntity)

let UpdateEntityWithClient client tableName (entity: 'a) = 
    let dynEntity = entity |> box :?> TableEntity
    doTableAction client tableName (TableOperation.Replace dynEntity)

let CreateOrUpdateEntityWithClient client tableName (entity: 'a) = 
    let dynEntity = entity |> box :?> TableEntity
    doTableActionWithCreate client tableName (TableOperation.InsertOrReplace dynEntity)

let DeleteTableWithClient (client:CloudTableClient) (tableName:string) = 
    let table = client.GetTableReference(tableName)
    table.DeleteIfExistsAsync() |> Async.AwaitTask

let CreateTableWithClient (client:CloudTableClient) (tableName:string) = 
    let table = client.GetTableReference(tableName)
    table.CreateIfNotExistsAsync() |> Async.AwaitTask

let CreateEntity (tableName:string) (entity: 'a) = 
    let client = BuildTableClient()
    CreateEntityWithClient client tableName entity 

let UpdateEntity (tableName:string) (newEntity: 'a) = 
    let client = BuildTableClient()
    UpdateEntityWithClient client tableName newEntity

let DeleteEntity (tableName:string) (entity: 'a) = 
    let client = BuildTableClient()
    DeleteEntityWithDataContext client tableName entity