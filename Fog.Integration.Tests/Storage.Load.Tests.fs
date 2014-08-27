module Storage.Load.Tests

open System
open System.Text
open NUnit.Framework
open FsUnit
open Microsoft.WindowsAzure
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.ServiceRuntime
open Fog.Core
open Fog.Storage.Queue
open Fog.Storage.Table
open Fog.Storage.Blob
open System.Data.Services.Common

[<DataServiceKey("PartitionKey", "RowKey")>]
type TestRecord(p, r, n) = 
    new() = TestRecord("","","")
    member val PartitionKey = p with get, set
    member val RowKey = r with get, set
    member val Name = n with get, set

let RunBlobTest() =
    [1..1000]
    |> Seq.iter
           (fun n -> 
               let message = sprintf "This is a test %i" n
               UploadBlob "testcontainerload" "testblobload" message |> Async.RunSynchronously |> ignore
               DeleteBlob "testcontainerload" "testblobload" |> Async.RunSynchronously |> ignore
          )

let RunTableTest() =
    [1..1000]
    |> Seq.iter
           (fun n -> 
               let testRecord = TestRecord("TestPart", Guid.NewGuid().ToString(), "Name" )
               CreateEntity "testtableload" testRecord |> Async.RunSynchronously |> ignore
               DeleteEntity "testtableload" testRecord |> Async.RunSynchronously |> ignore
          )

let RunQueueTest() =
    [1..1000]
    |> Seq.iter
           (fun n -> 
               let message = sprintf "This is a test %i" n
               AddMessage "testqueue" message |> Async.RunSynchronously |> ignore
               let result = GetMessages "testqueue" 20 5 |> Async.RunSynchronously
               for m in result do
                   DeleteMessage "testqueue" m |> Async.RunSynchronously
          )


