// Namespace: download/azure/main.go

package azure

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"../../database" // DB Handler Package
	"../../helpers"  // Helper Package

	"code.cloudfoundry.org/bytefmt"                            // Byte Format
	"github.com/Azure/azure-pipeline-go/pipeline"              // Azure Pipeline
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob" // Azure Blob Package
	"github.com/schollz/progressbar"                           // Progress Bar
)

// Global Constant(s)
const (
	statusCompleted = 1
	statusFailed    = 2
)

// EnvVars Struct
type EnvVars struct {
	AccountName, AccountKey, MediaFolder string
}

// Run - Entry Point for Azure Content Download
func Run(env EnvVars) bool {

	fmt.Println("Azure Content Download...")
	initiateDownload(env)

	return true
}

// Initiate Download of Azure Data
//
// @param env EnvVars struct
// @return nil
func initiateDownload(env EnvVars) {
	var wg sync.WaitGroup // Checks if traversing gets completed.

	// Getting Pending Download from Sync Table
	syncList := database.GetPendingAzureContent()

	// Initializing Empty Download Queue
	downloadQueue := make(map[string]string)

	// Creating Queue
	for idx := 0; idx < len(syncList); idx++ {
		downlodedBlob := helpers.ProcessBlobName(syncList[idx]["container"], syncList[idx]["blob"])
		downloadQueue[syncList[idx]["blob"]] = downlodedBlob
	}

	// Processing 10 Media Files Concurrently
	for idx := 0; idx < len(syncList); idx++ {
		go startWorker(&wg, syncList[idx], downloadQueue, env)
		wg.Add(1)
	}

	// Waiting for worker to finish download.
	wg.Wait()

	// Recursion Implementation:
	if len(syncList) != 0 {
		fmt.Println("-------------------------------------------------------")
		fmt.Println("[Recursion] Fetching New Data...")
		fmt.Println("[Time]", time.Now().Local())
		fmt.Println("-------------------------------------------------------")
		initiateDownload(env)
	}
}

// Start Worker to Download Blob
//
// @param wg WaitGroup, syncContent Maps, downloadQueue Maps, env EnvVars struct
// @return nil
func startWorker(wg *sync.WaitGroup, syncContent map[string]string, downloadQueue map[string]string, env EnvVars) {
	defer wg.Done() // Work Completed

	fmt.Println("Starting: ", syncContent["container"], "->", syncContent["blob"])

	// From the Azure portal, get your Storage account blob service URL endpoint.
	accountName, accountKey, mediaFolder := env.AccountName, env.AccountKey, env.MediaFolder
	containerName, blobName := syncContent["container"], syncContent["blob"]
	fileName := downloadQueue[blobName]

	/* [START] Handling Interrupt Signal */
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		for _, delFileName := range downloadQueue {
			os.Remove(mediaFolder + delFileName)
		}

		os.Exit(0)
	}()
	/* [END] Handling Interrupt Signal */

	// Create a default request pipeline using your storage account name and account key.
	azurePipeline := azblob.NewPipeline(azblob.NewSharedKeyCredential(accountName, accountKey), azblob.PipelineOptions{})

	// Create a BlobURL object to a blob in the container (we assume the container & blob already exist).
	azureURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, containerName, blobName))
	blobURL := azblob.NewBlobURL(*azureURL, azurePipeline)

	contentLength := int64(0) // Used for progress reporting to report the total number of bytes being downloaded.

	// NewGetRetryStream creates an intelligent retryable stream around a blob; it returns an io.ReadCloser.
	retryStream := azblob.NewDownloadStream(context.Background(),
		// We pass more tha "blobUrl.GetBlob" here so we can capture the blob's full
		// content length on the very first internal call to Read.
		func(ctx context.Context, blobRange azblob.BlobRange, ac azblob.BlobAccessConditions, rangeGetContentMD5 bool) (*azblob.GetResponse, error) {
			get, err := blobURL.GetBlob(ctx, blobRange, ac, rangeGetContentMD5)
			if err == nil && contentLength == 0 {
				// If 1st successful Get, record blob's full size for progress reporting
				contentLength = get.ContentLength()
			}
			return get, err
		},
		azblob.DownloadStreamOptions{})

	// NewResponseBodyStream wraps the GetRetryStream with progress reporting; it returns an io.ReadCloser.
	stream := pipeline.NewResponseBodyProgress(retryStream,
		func(bytesTransferred int64) {
			bar := progressbar.NewOptions(int(contentLength),
				progressbar.OptionEnableColorCodes(true),
				progressbar.OptionSetBytes(100),
				progressbar.OptionSetWidth(15),
				progressbar.OptionSetDescription("[cyan]"+blobName+" {"+bytefmt.ByteSize(uint64(bytesTransferred))+"/"+bytefmt.ByteSize(uint64(contentLength))+"}[reset]"),
				progressbar.OptionSetTheme(progressbar.Theme{
					Saucer:        "[green]=[reset]",
					SaucerHead:    "[green]>[reset]",
					SaucerPadding: " ",
					BarStart:      "[",
					BarEnd:        "]",
				}))

			bar.Add(int(bytesTransferred))
		})
	defer stream.Close() // The client must close the response body when finished with it

	// Create the file to hold the downloaded blob contents.
	file, _ := os.Create(mediaFolder + fileName)
	defer file.Close()

	// Write to the file by reading from the blob (with intelligent retries).
	_, downloadErr := io.Copy(file, stream)
	if downloadErr != nil { // Handling Download Error
		fmt.Print("Download Error!!")
		delete(downloadQueue, blobName)
		database.SetAzureFlag(containerName, blobName, statusFailed, downloadErr.Error())
		os.Remove(mediaFolder + fileName) // Deleting Corrupt File
	} else { // Download Completed
		fmt.Println("\n[Completed]: ", blobName)
		delete(downloadQueue, blobName)
		database.SetAzureFlag(containerName, blobName, statusCompleted, "")
	}

	fmt.Println("Pending File(s): ", downloadQueue)
}
