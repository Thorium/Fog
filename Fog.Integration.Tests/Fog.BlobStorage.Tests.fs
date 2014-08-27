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
    let blob = GetBlobReferenceInContainer container "testblob" |> Async.RunSynchronously
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
   GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> ignore
   let container = GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> Async.RunSynchronously
   container.Name |> should equal "testcontainer"

let ``It should delete an existing blob storage container``() =
   GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> Async.RunSynchronously
   |> DeleteBlobContainer |> Async.RunSynchronously

let ``It should upload specified text``() =
   let container = GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> Async.RunSynchronously
   UploadBlobToContainer container "testblob" "This is a test" |> Async.RunSynchronously |> ignore
   DownloadBlobFromContainer<string> container "testblob" |> Async.RunSynchronously |> should equal "This is a test"

let ``It should upload a specified byte array``() =
   let testBytes = Encoding.ASCII.GetBytes("This is a test")
   let container = GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> Async.RunSynchronously
   UploadBlobToContainer container  "testblob" testBytes |> Async.RunSynchronously |> ignore 
   DownloadBlobFromContainer<byte[]> container "testblob" |> Async.RunSynchronously |> should equal testBytes

let ``It should upload a specified file by name``() =
   let container = GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> Async.RunSynchronously
   using (File.OpenRead("test.xml")) <| fun f -> UploadBlobToContainer container "testblob" f |> ignore
   using (new MemoryStream()) 
       <| fun s -> (DownloadBlobStreamFromContainer container "testblob" s |> Async.RunSynchronously
                    use sr = new StreamReader(s)
                    sr.ReadToEnd() |> should equal "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\r\n<test/>\r\n")

let ``It should delete an existing blob``() =
   let container = GetBlobContainer <| BuildBlobClient() <| "testcontainer" |> Async.RunSynchronously
   UploadBlobToContainer container "testblob" "This is a test" |> Async.RunSynchronously |> ignore
   DeleteBlobFromContainer container "testblob" |> Async.RunSynchronously |> ignore

let ``It should upload specified text with friendlier syntax``() =
   UploadBlob "testcontainer" "testblob" "This is a test" |> Async.RunSynchronously |> ignore
   DownloadBlob<string> "testcontainer" "testblob" |> Async.RunSynchronously |> should equal "This is a test"

let ``It should upload a specified byte array with friendlier syntax``() =
   let testBytes = Encoding.ASCII.GetBytes("This is a test")
   UploadBlob "testcontainer" "testblob" testBytes |> Async.RunSynchronously |> ignore
   DownloadBlob<byte[]> "testcontainer" "testblob"  |> Async.RunSynchronously |> should equal testBytes

let ``It should upload a specified file by name with friendlier syntax``() =
   using (File.OpenRead("test.xml")) <| fun f -> UploadBlob "testcontainer" "testblob" f |> ignore
   using (new MemoryStream()) 
       <| fun s -> (DownloadBlobStream "testcontainer" "testblob" s |> Async.RunSynchronously
                    use sr = new StreamReader(s)
                    sr.ReadToEnd() |> should equal "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\r\n<test/>\r\n")

let ``It should delete an existing blob with friendlier syntax``() =
   UploadBlob "testcontainer" "testblob" "This is a test" |> Async.RunSynchronously |> ignore
   DeleteBlob "testcontainer" "testblob" |> Async.RunSynchronously |> ignore

let ``It should allow association of metadata for a blob``() =
    UploadBlob "testcontainer" "testblob" "This is a test" |> Async.RunSynchronously
    |> SetBlobMetadata ["testmeta", "Test"]    
    let metadata = GetBlobReference "testcontainer" "testblob" |> Async.RunSynchronously
                   |> GetBlobMetadata
    metadata.["testmeta"] |> should equal "Test"

// TODO:
// 5. Add async version of the most important functions (i.e. download/upload) -> Might wait until VS11 support is added. These will likely be provided OOTB.
// 6. Make all downloads and uploads run in parallel? -> Might wait until VS11 support is added.

let RunAll () = 
    //``It should work with a straight port of C# code``()    
    //``It should create a blob storage client with a convention based connectionString``()
    //``It should create a blob storage client with a provided connectionString``()
    //``It should create a blob storage container``()
    //``It should get an existing blob storage container``()
    //``It should delete an existing blob storage container``()
    //``It should upload a specified byte array``()
    //``It should upload specified text``()    
    //``It should upload a specified file by name``()
    //``It should delete an existing blob``()
    ``It should upload specified text with friendlier syntax``()
    ``It should upload a specified byte array with friendlier syntax``()
    //``It should upload a specified file by name with friendlier syntax``()
    //``It should delete an existing blob with friendlier syntax``()
    //``It should allow association of metadata for a blob``()