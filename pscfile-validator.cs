using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ManageAzStorageApp
{
    public static class PSCFileValidator
    {
        private static readonly CloudStorageAccount cloudStorageAccount;
        private static readonly CloudBlobClient cloudBlobClient;

        private static readonly string destinationContainer;
        static PSCFileValidator()
        {
            cloudStorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("DestinationStorage"));
            cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            destinationContainer = $"{Environment.GetEnvironmentVariable("DestinationContainer")}/{DateTime.Now.Date.Year}/{DateTime.Now.Date.Month.ToString("d2")}/PSC";

        }
        [FunctionName("invoicing-validatepscfiles")]
        public static async Task Run([BlobTrigger("invoicingfiles/{yyyy}/{MM}/PSC/{name}.zip", Connection = "DestinationStorage")] CloudBlockBlob myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# BlobTrigger Function Processed blob\n Name:{name}");
            try
            {
                CloudBlobContainer container = cloudBlobClient.GetContainerReference(destinationContainer);

                using (MemoryStream blobMemStream = new MemoryStream())
                {
                    await myBlob.DownloadToStreamAsync(blobMemStream);

                    using (ZipArchive archive = new ZipArchive(blobMemStream))
                    {
                        // check if the zip file and one txt 
                        if (archive.Entries.Count == 2 && archive.Entries.Any(x => x.FullName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase)))
                        {
                            foreach (ZipArchiveEntry entry in archive.Entries)
                            {
                                //Replace all NO digits, letters, or "-" by a "-" Azure storage is specific on valid characters
                                string fileName = Regex.Replace(entry.Name, @"[^a-zA-Z0-9\-]", "-").ToLower();
                                CloudBlockBlob blockBlob = container.GetBlockBlobReference($"valid/{fileName}");

                                using (var fileStream = entry.Open())
                                {
                                    if (!entry.FullName.EndsWith(".txt"))
                                    {
                                        log.LogInformation($"uploading {entry.FullName}");
                                        await blockBlob.UploadFromStreamAsync(fileStream);
                                    }
                                    else
                                    {
                                        log.LogInformation($"validating {entry.FullName}");

                                        string content = string.Empty;
                                        var reader = new StreamReader(fileStream);
                                        // Validate the headers..
                                        if (reader.EndOfStream)
                                        {
                                            log.LogInformation($"Validation Failed - Empty File received; File copied to  - '{destinationContainer}/Invalid' for the month of {DateTime.Now.Year}-{DateTime.Now.Month}");
                                          //  await CopyFilestoInvalidFolder(name, myBlob);
                                            break;
                                        }
                                        else
                                        {
                                            string line = reader.ReadLine();
                                            string[] fileHeaders = line.Split('\t');
                                            string[] validHeaders = { "JobNo", "JobDate", "SiteId", "Office", "FileName", "ServiceCode", "Units", "Description" };
                                            if (fileHeaders.Length == 0 || validHeaders.Intersect(fileHeaders).Count() <= 0)
                                            {
                                                log.LogInformation($"Validation Failed - PSC file received doesn't have valid headers;File copied to - '{destinationContainer}/Invalid' for the month of {DateTime.Now.Year}-{DateTime.Now.Month}");
                                              //  await CopyFilestoInvalidFolder(name, myBlob);
                                                break;
                                            }
                                            else
                                            {
                                                content = reader.ReadToEnd();
                                                reader.Close();
                                                content = Regex.Replace(content, "REVPAY-RECS-OH", "PESTMTS");
                                                content = Regex.Replace(content, "REVPAY-RECS-AZ", "PESTMTS");
                                                content = Regex.Replace(content, "REVPAY-EDEL-OH", "PESTMTS");
                                                content = Regex.Replace(content, "REVPAY-EDEL-AZ", "PESTMTS");

                                                byte[] byteArray = Encoding.ASCII.GetBytes(content);
                                                MemoryStream stream = new MemoryStream(byteArray);
                                                log.LogInformation($"uploading {entry.FullName}");
                                                await blockBlob.UploadFromStreamAsync(stream);
                                            }
                                        }
                                    }
                                }
                            }
                            log.LogInformation($"PSC files Validated and copied to path- '{destinationContainer}/valid'  for the month of {DateTime.Now.Year}-{DateTime.Now.Month}");
                        }
                        else
                        {
                            log.LogInformation($"Validation Failed for PSC Files and copied to path - '{destinationContainer}/Invalid' for the month of {DateTime.Now.Year}-{DateTime.Now.Month}");
                        }
                    }
                }
                // Delete the Zip file from actual location...
                log.LogInformation($"Deleting the original zip file - {name}");
                await DeleteBlobAsync(myBlob);
            }

            catch (Exception ex)
            {
                log.LogInformation($"Error! Something went wrong: {ex.Message}");
            }
        }

        //deletes blob...
        private static async Task<bool> DeleteBlobAsync(CloudBlockBlob currblob)
        {
            var result = await currblob.DeleteIfExistsAsync();
            return result;
        }

        // copy zip to unprocessed folder...
//         private static async Task CopyFilestoInvalidFolder(string name, CloudBlockBlob currblob)
//         {
//             CloudBlobContainer container = cloudBlobClient.GetContainerReference(destinationContainer);
//             CloudBlockBlob blockBlob = container.GetBlockBlobReference($"Invalid/{currblob.Name}");
//            using (var fileStream = System.IO.File.OpenWrite(@"path\myfile"))
// {
//     blockBlob.DownloadToStream(fileStream);
// }
//         }

    }
}

