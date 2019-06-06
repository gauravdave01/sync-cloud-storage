// Namespace: upload/s3/main.go

package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"os"
	"sync"

	"../../database" // DB Handler Package
	"../../helpers"  // Helper Package

	"github.com/aws/aws-sdk-go/aws"                  // AWS Core SDK
	"github.com/aws/aws-sdk-go/aws/session"          // Maintains AWS Session
	"github.com/aws/aws-sdk-go/service/s3/s3manager" // AWS S3 Manager (Upload/Upload Data)
)

// Global Constant(s)
const (
	statusCompleted = 1
	statusFailed    = 2
)

// EnvVars Struct
type EnvVars struct {
	AWSKey, AWSSecret, AWSBucket, AWSRegion string
	MediaFolder                             string
}

// Run - Entry Point for Azure Content Upload to S3
func Run(env EnvVars) bool {
	fmt.Println("Upload Azure Content to S3...")

	initiateUpload(env)

	return true
}

// Initiate Upload of Azure Data
//
// @param env EnvVars struct
// @return nil
func initiateUpload(env EnvVars) {
	var wg sync.WaitGroup // Checks if traversing gets completed.

	// Getting Pending Upload from Sync Table
	syncList := database.GetPendingS3Content()

	// Initializing Empty Upload Queue
	uploadQueue := make(map[string]string)

	// Creating Queue
	for idx := 0; idx < len(syncList); idx++ {
		downlodedBlob := helpers.ProcessBlobName(syncList[idx]["container"], syncList[idx]["blob"])
		uploadQueue[syncList[idx]["blob"]] = downlodedBlob
	}

	// Processing 10 Media Files Concurrently
	for idx := 0; idx < len(syncList); idx++ {
		go startWorker(&wg, syncList[idx], uploadQueue, env)
		wg.Add(1)
	}

	// Waiting for worker to finish upload.
	wg.Wait()

	// Recursion Implementation:
	if len(syncList) != 0 {
		fmt.Println("-------------------------------------------------------")
		fmt.Println("[Recursion] Fetching New Data...")
		fmt.Println("-------------------------------------------------------")
		initiateUpload(env)
	}
}

// Start Worker to Upload Blob
//
// @param wg WaitGroup, syncContent Maps, uploadQueue Maps, env EnvVars struct
// @return nil
func startWorker(wg *sync.WaitGroup, syncContent map[string]string, uploadQueue map[string]string, env EnvVars) {
	defer wg.Done() // Work Completed

	fmt.Println("Uploading: ", syncContent["blob"])

	// From the Azure portal, get your Storage account blob service URL endpoint.
	awsKey, awsSecret, awsBucket, awsRegion := env.AWSKey, env.AWSSecret, env.AWSBucket, env.AWSRegion
	mediaFolder := env.MediaFolder
	containerName, blobName := syncContent["container"], syncContent["blob"]

	file, err := os.Open(mediaFolder + uploadQueue[blobName])
	exitErrorf(err, "Unable to open file %q, %v", err)

	defer file.Close()

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsKey, awsSecret, ""),
	})
	exitErrorf(err, "Session Error", err)

	uploader := s3manager.NewUploader(sess)

	resp, uploadErr := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(awsBucket),
		Key:    aws.String(uploadQueue[blobName]),
		Body:   file,
		ACL:    aws.String("public-read"),
	}, func(u *s3manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10MB part size
		u.LeavePartsOnError = true    // Don't delete the parts if the upload fails.
		u.Concurrency = 20
	})

	if uploadErr != nil {
		fmt.Println("[Upload Error]", blobName)
		delete(uploadQueue, blobName)
		database.SetS3Flag(containerName, blobName, statusFailed, uploadErr.Error())
	} else { // Upload Completed
		fmt.Println("\n[Completed]: ", blobName)

		database.SetS3Flag(containerName, blobName, statusCompleted, "")
		// os.Remove(mediaFolder + uploadQueue[blobName]) // Delete Locale File it gets uploaded.
		delete(uploadQueue, blobName)

		fmt.Println("[S3] File Path", resp.Location) // File Path
	}

	fmt.Println("Pending File(s): ", uploadQueue)
}

// Error Handling
func exitErrorf(err error, msg string, args ...interface{}) {
	if err != nil {
		fmt.Fprintf(os.Stderr, msg+"\n", args...)
		os.Exit(1)
	}
}
