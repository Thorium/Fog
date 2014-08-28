module Fog.Storage.Table.Tests

open System
open NUnit.Framework
open FsUnit
open Microsoft.WindowsAzure
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.ServiceRuntime
open System.Data.Services.Common
open System.Text
open Fog.Core
open Fog.Storage.Table
open System.IO
open Microsoft.FSharp.Linq.Query

type TestRecord(p, r, n) =
    inherit TableEntity(p, r)
    new(name) = TestRecord("TestPart", Guid.NewGuid().ToString(), name)
    new() = TestRecord("")
    member val Name = n with get, set

let ``It should create a table storage client with a convention based connectionString``() = 
    BuildTableClient().BaseUri.AbsoluteUri |> should equal "http://127.0.0.1:10002/devstoreaccount1"

let ``It should create a table storage client with a provided connectionString``() = 
    let client = (BuildTableClientWithConnStr "TestTableStorageConnectionString")
    client.BaseUri.AbsoluteUri |> should equal "http://127.0.0.1:10002/devstoreaccount1"

let ``It should add a record to a specified table``() = 
    let client = BuildTableClient()
    let testRecord = TestRecord("test")
    CreateEntityWithClient client "testtable" testRecord |> Async.RunSynchronously |> ignore
    let table = client.GetTableReference("testtable")

    let query = 
        (TableQuery<TestRecord>())
            .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, testRecord.PartitionKey))
            .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, testRecord.RowKey))                
    let result = table.ExecuteQuery<TestRecord>(query) |> Seq.head
    result.Name |> should equal "test"

let ``It should allow updating a record``() = 
    async {
        let client = BuildTableClient()
        let table = client.GetTableReference("testtable")
        let testRecord = TestRecord("test")
        let! result1 = CreateEntityWithClient client "testtable" testRecord
        let query1 =
            (TableQuery<TestRecord>())
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, testRecord.PartitionKey))
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, testRecord.RowKey))
        let result2 = table.ExecuteQuery<TestRecord>(query1) |> Seq.head
    
        let updateRecord = testRecord 
        updateRecord.Name <- "test2"
        let! result = UpdateEntityWithClient client "testtable" updateRecord
        let query2 =
            (TableQuery<TestRecord>())
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, testRecord.PartitionKey))
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, testRecord.RowKey))
        let result3 = table.ExecuteQuery<TestRecord>(query2) |> Seq.head

        result3.Name |> should equal "test2"
    } |> Async.RunSynchronously

let ``It should allow deleting a record``() = 
    async {
        let client = BuildTableClient()
        let testRecord = TestRecord( PartitionKey = "TestPart", RowKey = Guid.NewGuid().ToString(), Name = "test" )
        let! result1 = CreateEntityWithClient client "testtable" testRecord
        let table = client.GetTableReference("testtable")
        let query = 
            (TableQuery<TestRecord>())
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, testRecord.PartitionKey))
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, testRecord.RowKey))                
        let resultRecord = table.ExecuteQuery<TestRecord>(query) |> Seq.head
        let! result2 = DeleteEntityWithDataContext client "testtable" resultRecord
        ()
    } |> Async.RunSynchronously

let ``It should allow creating and deleting a table``() = 
    async {
        let client = BuildTableClient()
        let table = client.GetTableReference("testtable2")
        table.Exists() |> should equal false
        let! res1 = CreateTableWithClient client "testtable2"
        table.Exists() |> should equal true
        let! res2 = DeleteTableWithClient client "testtable2"
        table.Exists() |> should equal false
    } |> Async.RunSynchronously

let ``It should allow easy creation of a record``() =
    let testRecord = TestRecord( PartitionKey = "TestPart", RowKey = Guid.NewGuid().ToString(), Name = "test" )
    CreateEntity "testtable" testRecord |> Async.RunSynchronously |> ignore
    let client = BuildTableClient()
    let table = client.GetTableReference("testtable")
    let query = 
        (TableQuery<TestRecord>())
            .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, testRecord.PartitionKey))
            .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, testRecord.RowKey))                
    let result = table.ExecuteQuery<TestRecord>(query) |> Seq.head
    result.Name |> should equal "test"

let ``It should allow easy update of a record``() =
    async {
        let originalRecord = TestRecord( PartitionKey = "TestPart", RowKey = Guid.NewGuid().ToString(), Name = "test" )
        let! res1 = CreateEntity "testtable" originalRecord 
        let newRecord = originalRecord
        newRecord.Name <- "test2"
        let! res2 = UpdateEntity "testtable" newRecord
        let client = BuildTableClient()
        let table = client.GetTableReference("testtable")
        let query = 
            (TableQuery<TestRecord>())
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, newRecord.PartitionKey))
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, newRecord.RowKey))                
        let result = table.ExecuteQuery<TestRecord>(query) |> Seq.head
        result.Name |> should equal "test2"
    } |> Async.RunSynchronously

// TODO:
// 4. Make all downloads and uploads run in parallel? -> Might wait until VS11 support is added.
// 5. This gets improved once we can use F# 3.0. OData Type Provider, OOTB Query syntax, etc. make this even easier. Need to build some of that in when VS11 support is added.
// (6. Support for POCO-objects via DynamicTableEntity? POCOs seems to work now via casting.)

let RunAll () = 
    ``It should create a table storage client with a convention based connectionString``()
    ``It should create a table storage client with a provided connectionString``()
    ``It should add a record to a specified table``()
    ``It should allow deleting a record``()
    ``It should allow updating a record``()
    ``It should allow creating and deleting a table``()
    ``It should allow easy creation of a record``()
    ``It should allow easy update of a record``()