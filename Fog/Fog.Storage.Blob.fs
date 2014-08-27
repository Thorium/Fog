module Fog.Storage.Blob

open Microsoft.WindowsAzure.Storage.Blob
open System.IO
open Fog.Core
open System.Text

let BuildBlobClientWithConnStr(connectionString) =
    memoize (fun conn -> 
               let storageAccount = GetStorageAccount conn
               storageAccount.CreateCloudBlobClient() ) connectionString

let BuildBlobClient() = BuildBlobClientWithConnStr "BlobStorageConnectionString"

let GetBlobContainer (client:CloudBlobClient) (containerName:string) = 
    let container = client.GetContainerReference <| containerName.ToLower()
    async {
        let! ok = container.CreateIfNotExistsAsync() |> Async.AwaitTask
        return container
    }

let DeleteBlobContainer (blobContainer:CloudBlobContainer) = 
    blobContainer.DeleteIfExistsAsync() |> Async.AwaitTask

let GetBlobReferenceInContainer (container:CloudBlobContainer) (name:string) : Async<ICloudBlob> = 
    container.GetBlobReferenceFromServerAsync <| name.ToLower() |> Async.AwaitTask

let GetBlobReference (containerName:string) name : Async<ICloudBlob> = 
    async{ 
        let! container = GetBlobContainer <| BuildBlobClient() <| containerName
        return! GetBlobReferenceInContainer container name
    }

let UploadBlobToContainer<'a> (container:CloudBlobContainer) (blobName:string) (item:'a) = 
    let doTask = Async.AwaitIAsyncResult >> Async.Ignore
    async {
        let! blob = GetBlobReferenceInContainer container blobName
        match box item with
        | :? Stream as s -> 
            do! blob.UploadFromStreamAsync s |> doTask
        | :? (byte[]) as b -> 
            do! blob.UploadFromByteArrayAsync(b, 0, b.Length) |> doTask
        | :? string as str -> 
            let enc = Encoding.ASCII.GetBytes(str)
            use ms = new MemoryStream(enc)
            do! blob.UploadFromStreamAsync ms |> doTask
        | _ -> 
            failwith "This type is not supported"
            use ms = new MemoryStream()
            let formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            formatter.Serialize(ms, item);
            do! blob.UploadFromStreamAsync ms |> doTask
        return blob
    }

let UploadBlob<'a> (containerName:string) (blobName:string) (item:'a) = 
    async{ 
        let! container = GetBlobContainer <| BuildBlobClient() <| containerName
        return! UploadBlobToContainer<'a> container blobName item
    }

let DownloadBlobStreamFromContainer (container:CloudBlobContainer) (blobName:string) (stream:#Stream) = 
    async{
        let! blob = GetBlobReferenceInContainer container blobName
        let! res = blob.DownloadToStreamAsync stream |> Async.AwaitIAsyncResult
        stream.Seek(0L, SeekOrigin.Begin) |> ignore
    }

let DownloadBlobStream containerName blobName (stream:#Stream) = 
    async{ 
        let! container = GetBlobContainer <| BuildBlobClient() <| containerName
        do! DownloadBlobStreamFromContainer container blobName stream
    }

let DownloadBlobFromContainer<'a> (container:CloudBlobContainer) (blobName:string) = 
    let doTask = Async.AwaitIAsyncResult >> Async.Ignore
    async{
        let! blob = GetBlobReferenceInContainer container blobName
        match typeof<'a> with
        | st when st = typeof<Stream> -> 
            use ms = new MemoryStream()
            do! blob.DownloadToStreamAsync(ms) |> doTask
            return ms :> Stream |> box :?> 'a
        | b when b = typeof<byte[]> -> 
            let ba: byte [] = Array.zeroCreate 0
            let! ok = blob.DownloadToByteArrayAsync(ba, 0) |> Async.AwaitTask
            return ba |> box :?> 'a
        | str when str = typeof<string> -> 
            use ms = new MemoryStream()
            do! blob.DownloadToStreamAsync(ms) |> doTask           
            return Encoding.ASCII.GetString(ms.ToArray()) |> box :?> 'a
        | _ -> 
            failwith "This type is not supported"
            use ms = new MemoryStream()
            do! blob.DownloadToStreamAsync(ms) |> doTask
            let formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            ms.Seek(0L, SeekOrigin.Begin) |> ignore
            match ms.Length with
            | 0L -> return Unchecked.defaultof<'a>
            | _ -> return formatter.Deserialize(ms) |> box :?> 'a
    }

let DownloadBlob<'a> (containerName:string) (blobName:string) = 
    async {
        let! container = GetBlobContainer <| BuildBlobClient() <| containerName
        return! DownloadBlobFromContainer<'a> container blobName
    }

let DeleteBlobFromContainer (container:CloudBlobContainer) (blobName:string) = 
    async {
        let! blob = GetBlobReferenceInContainer container blobName
        return! blob.DeleteIfExistsAsync() |> Async.AwaitTask
    }

let DeleteBlob (containerName) (blobName:string) = 
    async {
        let! container = GetBlobContainer <| BuildBlobClient() <| containerName
        return! DeleteBlobFromContainer container blobName
    }

let GetBlobMetadata (blobReference:ICloudBlob) = 
    blobReference.FetchAttributes()
    blobReference.Metadata

let SetBlobMetadata (metadata:list<string*string>) (blobReference:ICloudBlob) = 
    metadata |> Seq.iter(fun (k,v) -> blobReference.Metadata.Add(k, v))
    blobReference.SetMetadata()