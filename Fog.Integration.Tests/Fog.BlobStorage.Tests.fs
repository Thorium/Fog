module Fog.Storage.Blob.Tests

open NUnit.Framework
open FsUnit
open Microsoft.WindowsAzure
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.ServiceRuntime
open System.Text
open Fog.Core
open Fog.Storage.Blob
open System.IO

let ``It should work with a straight port of C# code``() =
    let storageAccount = 
        RoleEnvironment.GetConfigurationSettingValue "BlobStorageConnectionString" 
        |> CloudStorageAccount.Parse
    let client = storageAccount.CreateCloudBlobClient()
    let container = client.GetContainerReference("testcontainer")
    container.CreateIfNotExists() |> ignore
    let blob = GetBlobReferenceInContainer container "testblob"
    let ms = new MemoryStream("My super awesome text to upload" |> Encoding.ASCII.GetBytes)
    blob.UploadFromStream ms

let ``It should create a blob storage client with a convention based connectionString``() = 
    BuildBlobClient().BaseUri.AbsoluteUri |> should equal "http://127.0.0.1:10000/devstoreaccount1"

let ``It should create a blob storage client with a provided connectionString``() = 
    let client = (BuildBlobClientWithConnStr "TestBlobStorageConnectionString")
    client.BaseUri.AbsoluteUri |> should equal "http://127.0.0.1:10000/devstoreaccount1"

let ``It should create a blob storage container``() =
   let container = GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> Async.RunSynchronously
   container.Name |> should equal "testcontainer"

let ``It should get an existing blob storage container``() =
   async {
       let! cont = GetBlobContainer <| BuildBlobClient() <| "testcontainer"
       let! container = GetBlobContainer <| BuildBlobClient() <| "testcontainer"
       container.Name |> should equal "testcontainer"
   } |> Async.RunSynchronously

let ``It should delete an existing blob storage container``() =
   async {
       let! cont = GetBlobContainer <| BuildBlobClient() <| "testcontainer"
       let! ok = cont |> DeleteBlobContainer
       ()
   } |> Async.RunSynchronously

let ``It should upload specified text``() =
   async {
       let! container = GetBlobContainer <| BuildBlobClient() <| "testcontainer"
       let! blob = UploadBlobToContainer container "testblob" "This is a test" 
       let! result = DownloadBlobFromContainer<string> container "testblob" 
       result |> should equal "This is a test"
   } |> Async.RunSynchronously

let ``It should upload a specified byte array``() =
   async {
       let testBytes = Encoding.ASCII.GetBytes("This is a test")
       let! container = GetBlobContainer <| BuildBlobClient() <| "testcontainer"
       let! blob = UploadBlobToContainer container  "testblob" testBytes
       let! result = DownloadBlobFromContainer<byte[]> container "testblob" 
       result |> should equal testBytes
   } |> Async.RunSynchronously

let ``It should upload a specified file by name``() =
   let container = GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> Async.RunSynchronously
   using (File.OpenRead("test.xml")) <| fun f -> UploadBlobToContainer container "testblob" f |> Async.RunSynchronously |> ignore
   using (new MemoryStream()) 
       <| fun s -> (DownloadBlobStreamFromContainer container "testblob" s |> Async.RunSynchronously
                    use sr = new StreamReader(s)
                    sr.ReadToEnd() |> should equal "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\r\n<test/>\r\n")

let ``It should delete an existing blob``() =
   async {
       let! container = GetBlobContainer <| BuildBlobClient() <| "testcontainer"
       let! blob = UploadBlobToContainer container "testblob" "This is a test"
       let! del = DeleteBlobFromContainer container "testblob"
       ()
   } |> Async.RunSynchronously

let ``It should upload specified text with friendlier syntax``() =
   async {
       let! blob = UploadBlob "testcontainer" "testblob" "This is a test"
       let! result = DownloadBlob<string> "testcontainer" "testblob"
       result |> should equal "This is a test"
   } |> Async.RunSynchronously
   
let ``It should upload a specified byte array with friendlier syntax``() =
   let testBytes = Encoding.ASCII.GetBytes("This is a test")
   async {
       let! blob = UploadBlob "testcontainer" "testblobb" testBytes
       let! result = DownloadBlob<byte[]> "testcontainer" "testblobb"
       result |> should equal testBytes
   } |> Async.RunSynchronously

let ``It should upload a specified file by name with friendlier syntax``() =
   using (File.OpenRead("test.xml")) <| fun f -> UploadBlob "testcontainer" "testblobf" f |> Async.RunSynchronously |> ignore
   using (new MemoryStream()) 
       <| fun s -> (DownloadBlobStream "testcontainer" "testblobf" s |> Async.RunSynchronously
                    use sr = new StreamReader(s)
                    sr.ReadToEnd() |> should equal "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\r\n<test/>\r\n")

let ``It should delete an existing blob with friendlier syntax``() =
   async {
       let! blob = UploadBlob "testcontainer" "testblob" "This is a test"
       let! result = DeleteBlob "testcontainer" "testblob"
       ()
   } |> Async.RunSynchronously

let ``It should allow association of metadata for a blob``() =
   async {
        let! blob = UploadBlob "testcontainer" "testblob" "This is a test"
        blob |> SetBlobMetadata ["testmeta", "Test"]    
        let! refer = GetBlobReference "testcontainer" "testblob"
        let metadata = refer |> GetBlobMetadata
        metadata.["testmeta"] |> should equal "Test"
   } |> Async.RunSynchronously

// TODO:
// 1. Fix byte-array
// 2. Support batches, e.g. http://gauravmantri.com/2012/11/17/storage-client-library-2-0-migrating-table-storage-code/

// 5. This gets improved once we can use F# 3.0. OData Type Provider, OOTB Query syntax, etc. make this even easier. Need to build some of that in when VS11 support is added.
// 6. Make all downloads and uploads run in parallel? -> Might wait until VS11 support is added.

let RunAll () = 
    ``It should work with a straight port of C# code``()    
    ``It should create a blob storage client with a convention based connectionString``()
    ``It should create a blob storage client with a provided connectionString``()
    ``It should create a blob storage container``()
    ``It should get an existing blob storage container``()
    ``It should delete an existing blob storage container``()
    //``It should upload a specified byte array``()
    ``It should upload specified text``()    
    ``It should upload a specified file by name``()
    ``It should delete an existing blob``()
    ``It should upload specified text with friendlier syntax``()
    //``It should upload a specified byte array with friendlier syntax``()
    ``It should upload a specified file by name with friendlier syntax``()
    ``It should delete an existing blob with friendlier syntax``()
    ``It should allow association of metadata for a blob``()
