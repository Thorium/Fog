module Fog.Storage.Queue

open System
open Microsoft.WindowsAzure.Storage.Queue
open Fog.Core

let BuildQueueClientWithConnStr(connectionString) =
    memoize (fun conn -> 
                 let storageAccount = GetStorageAccount conn
                 storageAccount.CreateCloudQueueClient() ) connectionString 

let BuildQueueClient() = BuildQueueClientWithConnStr "QueueStorageConnectionString"

let GetQueueReference (client:CloudQueueClient) (queueName:string) =
    client.GetQueueReference <| queueName.ToLower()

let CreateQueueWithClient (client:CloudQueueClient) (queueName:string) =
    async {
        let queue = GetQueueReference client queueName
        let! ok = queue.CreateIfNotExistsAsync() |> Async.AwaitTask
        return queue
    }

let DeleteQueueWithClient (client:CloudQueueClient) (queueName:string) =
    let queue = GetQueueReference client queueName
    queue.DeleteIfExistsAsync() |> Async.AwaitTask

let AddMessageWithClient (client:CloudQueueClient) (queueName:string) content =
    async {
        let! queue = CreateQueueWithClient client queueName
        let! ok =
            match box content with
            | :? string as s -> queue.AddMessageAsync(CloudQueueMessage(s)) |> Async.AwaitIAsyncResult
            | :? (byte[]) as b -> queue.AddMessageAsync(CloudQueueMessage(b)) |> Async.AwaitIAsyncResult
            | _ -> failwith "The provided content is not of a support type (i.e. string or byte[]"
        return queue
    }

let DeleteMessageWithClient (client:CloudQueueClient) (queueName:string) (message:CloudQueueMessage) =
    async {
        let! queue = CreateQueueWithClient client queueName
        return! queue.DeleteMessageAsync(message) |> Async.AwaitIAsyncResult |> Async.Ignore
    }

let GetMessageWithClient (client:CloudQueueClient) (queueName:string) = 
    async {
        let! queue = CreateQueueWithClient client queueName
        return! queue.GetMessageAsync() |> Async.AwaitTask
    }

let GetMessagesWithClient (client:CloudQueueClient) (queueName:string) (messageCount:int) (ttlInMinutes:int) = 
    async {
        let! queue = CreateQueueWithClient client queueName
        let cancel = new System.Threading.CancellationTokenSource(ttlInMinutes*60*1000)
        return! queue.GetMessagesAsync(messageCount, cancel.Token) |> Async.AwaitTask
    }

let AddMessage (queueName:string) content  =
    let client = BuildQueueClient()
    AddMessageWithClient client queueName content

let GetMessage queueName =
    let client = BuildQueueClient()
    GetMessageWithClient client queueName

let GetMessages (queueName:string) (messageCount:int) (ttlInMinutes:int) = 
    let client = BuildQueueClient()
    GetMessagesWithClient client queueName messageCount ttlInMinutes

let DeleteMessage (queueName:string) (message:CloudQueueMessage) =
    let client = BuildQueueClient()
    DeleteMessageWithClient client queueName message
