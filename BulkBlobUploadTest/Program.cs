using Azure;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace BulkBlobUploadTest
{
  class Program
  {

    const string blobStorageUriString = "https://bulkuploadtestjaspecla.blob.core.windows.net/";
    const string blobContainerName = "upload3";

    /// <summary>
    /// A demonstration of using BlobUploadOptions to maximize upload throughput for bulk uploads,
    /// while still authenticating with DefaultAzureCredential.  
    /// 
    /// Note that whatever user or service principal is returned from DefaultAzureCredential must have 
    /// "Storage Blob Data Contributor" access to the container in question.  It is not sufficient to have
    /// Owner or Contributor permissions to the storage account.
    /// 
    /// Also note that this program will upload every file in your MyPictures folder in Windows.  So make
    /// sure those are files that you don't mind being uploaded.
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    static async Task Main(string[] args)
    {

      var blobStorageUri = new Uri(blobStorageUriString);
      var credentials = new DefaultAzureCredential();

      var blobServiceClient = new BlobServiceClient(blobStorageUri, credentials);

      var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);

      var imagesFilePath = Environment.GetFolderPath(Environment.SpecialFolder.MyPictures);

      var blobUploadOptions = new BlobUploadOptions
      {
        TransferOptions = new StorageTransferOptions
        {
          MaximumConcurrency = 8,
          MaximumTransferSize = 50 * 1024 * 1024
        }
      };

      var tasks = new Queue<Task<Response<BlobContentInfo>>>();

      int numFiles = 0;
      Stopwatch timer = Stopwatch.StartNew();
      foreach (string filePath in Directory.GetFiles(imagesFilePath))
      {
        string fileName = Path.GetFileName(filePath);
        var blob = blobContainerClient.GetBlobClient(fileName);

        tasks.Enqueue(blob.UploadAsync(filePath, blobUploadOptions));
        numFiles++;
      }

      await Task.WhenAll(tasks);
      timer.Stop();

      Console.WriteLine($"Uploaded {numFiles} files in {timer.Elapsed.TotalSeconds} seconds");


    }
  }
}
