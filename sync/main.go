// Namespace: sync/main.go

package sync

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"../database" // DB Handler Package
	"../helpers"  // Helper Package

	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob" // Azure Blob Package
	_ "github.com/mattn/go-sqlite3"                            // SQLite3 Connection
)

// Global Constant(s)
const (
	containerNotFound = 404
	blobNotFound      = 503
	traverseCompleted = 200
	liveContainer     = 100
)

// Global Variable(s)
var fileExceptionList = [3]string{".xml", ".ism", ".ismc"} // File extension which needs to be skipped
var containerExceptionList = [15]string{
	"test", "dummy", "others"} // Containers which needs to be skipped
var liveContainerList = [16]string{
	"employees-data", "employees-indentity",
	"videos", "images", "reports"} // Containers which are live and gets updated frequently

// EnvVars Struct
type EnvVars struct {
	AccountName, AccountKey string
	ContainerFlag, BlobFlag bool
}

// Handling Error
func handleErrors(err error, asset string) {
	if err != nil {
		log.Print("Container: ", asset)
		log.Fatal(err)

	}
}

// Rescue Operation
func rescue() {
	r := recover() // We stopped here the failing process!!!
	if r != nil {
		fmt.Println("Panic has been recover")
	}
}

// Run - Method to display the specified resource.
//
// @param env EnvVars struct
// @return boolean
func Run(env EnvVars) bool {
	fmt.Printf("Azure Container: Blob Sync Mechanism.\n")
	// defer rescue()

	if env.ContainerFlag { // Sync Container(s)
		syncContainer(env)
	} else if env.BlobFlag { // Sync Container: Blob Mapping
		syncBlob(env)
	} else { // Flag was missing.
		fmt.Println("Sync: Flag is Missing!")
		return false
	}

	return true // Success
}

// Sync Azure Container(s) to SQLite "containers" table
//
// @param env EnvVars struct
// @return boolean
func syncContainer(env EnvVars) {
	fmt.Println("Setting Up Container(s)")

	// Create a default request pipeline using your storage account name and account key.
	azurePipeline := azblob.NewPipeline(azblob.NewSharedKeyCredential(env.AccountName, env.AccountKey), azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	azureURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", env.AccountName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	serviceURL := azblob.NewServiceURL(*azureURL, azurePipeline)

	// List the container(s)
	containerCounter := 1
	for containerMarker := (azblob.Marker{}); containerMarker.NotDone(); {
		listContainer, err := serviceURL.ListContainers(context.Background(), containerMarker, azblob.ListContainersOptions{})
		handleErrors(err, "Container Listing API Failed!")

		// Saving Container Details
		for _, containerObject := range listContainer.Containers {
			if isValidContainer(containerExceptionList, containerObject.Name) {
				fmt.Printf("[%d]. Container: %s\n", containerCounter, containerObject.Name)
				database.InsertInContainer(containerObject.Name)
				containerCounter++ // Increment Counter
			} else {
				fmt.Println("Skipping: ", containerObject.Name)
			}
		}

		/* Sleep after every API request */
		fmt.Println("Sleep...")
		sleepFor(5) // 5 Sec Halt b/w API Call(s)
		fmt.Println("Woken...")
		/* Sleep after every API request */

		// ListContainers returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		containerMarker = listContainer.NextMarker
	}
}

// Sync Azure Container(s): Blob Mapping in SQLite "sync" table
//
// @param env EnvVars struct
// @return boolean
func syncBlob(env EnvVars) {
	// Getting Container Listing
	containers := database.GetPendingContainer()

	// Converting Data Slice to 2D Matrix Slice
	containerMatrix := helpers.CreateContainerMatrix(containers)

	// Container Channel
	containerChannel := make(chan int)

	// Processing 10x10 Matrix (row level)
	for idx := 0; idx < len(containerMatrix); idx++ {
		fmt.Println("Starting Container Set:")
		fmt.Println("Channel Set [#", idx, "]: ", containerMatrix[idx])

		go traverseContainerSet(containerChannel, containerMatrix[idx], env)
		<-containerChannel // Channel to mark Matrix row completed

		fmt.Println("Completed Container Set.")
	}

	// Recursion Implementation:
	if len(containers) != 0 {
		fmt.Println("[Recursion] Fetching New Data...")
		syncBlob(env)
	}
}

// Traverse Container Set of 10x10 Matrix
//
// @param containerChannel chan
// @param containerSet slice
// @param env EnvVars struct
// @return channel finished
func traverseContainerSet(containerChannel chan<- int, containerSet []string, env EnvVars) {
	var wg sync.WaitGroup // Checks if traversing gets completed.

	dbConnection := database.InitConnection()
	defer dbConnection.Close()

	// Processing Single Row of Channel Set
	for idx := 0; idx < len(containerSet); idx++ {
		fmt.Println("Starting Channel [#", idx, "]: ", containerSet[idx])
		wg.Add(1)

		go traverseContainerWorker(&wg, containerSet[idx], dbConnection, env)
	}

	// Waiting for container to finish.
	wg.Wait()

	// Container Completed
	containerChannel <- 1
}

// Worker to traverse container and save blob details
//
// @param wg WaitGroup
// @param containerName string
// @param dbConnection pointer
// @param env EnvVars struct
// @return channel finished
func traverseContainerWorker(wg *sync.WaitGroup, containerName string, dbConnection *sql.DB, env EnvVars) {
	defer wg.Done() // Work Completed

	// Default Variable(s)
	var updateErr error
	var fileCount = 0
	var containerStatus = 0

	// Create a default request pipeline using your storage account name and account key.
	azurePipeline := azblob.NewPipeline(azblob.NewSharedKeyCredential(env.AccountName, env.AccountKey), azblob.PipelineOptions{})

	// Configuring Azure Container Details API
	containerURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", env.AccountName, containerName))

	// Initializing Azure Container Details API
	fmt.Printf("Container: %s\n", containerName)
	containerServiceURL := azblob.NewContainerURL(*containerURL, azurePipeline)

	// Container to Blob Listing
	for blobMarker := (azblob.Marker{}); blobMarker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerServiceURL.ListBlobs(context.Background(), blobMarker, azblob.ListBlobsOptions{MaxResults: 20})

		// Azure Specific Error Handling
		if err != nil {
			if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
				if serr.ServiceCode() == azblob.ServiceCodeContainerNotFound { // Compare serviceCode to ServiceCodeXxx constants
					fmt.Println("Container: ", containerName, " not found!")
					// Updating status flag in containers table
					updateContainerQuery, _ := dbConnection.Prepare("UPDATE containers SET status = ? WHERE name = ?")
					containerStatus = containerNotFound
					_, updateErr = updateContainerQuery.Exec(containerStatus, containerName)

					if updateErr != nil {
						fmt.Println("[Failed] Setting Completed Flag For: ", containerName)
						fmt.Println("[Reason] Container Not Found")
					}

					break // Skips Loop
				}
			} else {
				handleErrors(err, containerName)
			}
		}

		// ListBlobs returns the start of the next segment (used for pagination purpose)
		blobMarker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			blobFileExt := filepath.Ext(blobInfo.Name)

			if isValidExtension(fileExceptionList, blobFileExt) {
				fileCount++ // File Counter

				var count int
				stmt, _ := dbConnection.Prepare("select count(*) from sync where container=? and blob=?")
				stmt.QueryRow(containerName, blobInfo.Name).Scan(&count)

				if count == 0 {
					// Preparing Statement
					insertStatement, statementError := dbConnection.Prepare("INSERT INTO sync(container, blob, created_at) values(?,?,?)")
					handleErrors(statementError, "Insert Container Prepare Failed")

					// Executing Statement
					insertResponse, insertError := insertStatement.Exec(containerName, blobInfo.Name, time.Now().Local())
					if insertError == nil {
						// Fetch Last Insert ID
						id, dbError := insertResponse.LastInsertId()
						handleErrors(dbError, strconv.FormatInt(id, 10))

						fmt.Println("Container: ", containerName, "| Blob: ", blobInfo.Name)
						fmt.Println("Mapping Completed!")
					}
					// handleErrors(insertError, "Insert Container Execute Failed")
				} else {
					fmt.Println("[Skipping] ", containerName, "-->", blobInfo.Name, " already exists.")
				}
			}
		}
	}

	// Updating status flag in containers table
	updateContainerQuery, _ := dbConnection.Prepare("UPDATE containers SET status = ? WHERE name = ?")

	// Checking if container was empty
	if fileCount == 0 {
		containerStatus = blobNotFound
	} else {
		if isLiveContainer(liveContainerList, containerName) {
			containerStatus = liveContainer
		} else {
			containerStatus = traverseCompleted
		}
	}

	// Updating Container Status after Process Completion
	_, updateErr = updateContainerQuery.Exec(containerStatus, containerName)
	if updateErr != nil {
		fmt.Println("[Failed] Setting Completed Flag For: ", containerName)
	}

	fmt.Println("[Success] Setting Completed Flag For: ", containerName)
}

// Method to check if container doesn't belongs to live list.
//
// @param liveContainerList slice
// @param containerName string
// @return boolean
func isLiveContainer(liveContainerList [16]string, containerName string) bool {
	for _, liveContainer := range liveContainerList {
		if liveContainer == containerName {
			return true // Live
		}
	}

	return false // Still
}

// Method to check if container doesn't belongs to exceptions list.
//
// @param containerExceptionList slice
// @param containerName string
// @return boolean
func isValidContainer(containerExceptionList [15]string, containerName string) bool {
	for _, exceptContainer := range containerExceptionList {
		if exceptContainer == containerName {
			return false // Match
		}
	}

	return true // Un-Match
}

// Method to check if file extension doesn't belongs to exceptions list.
//
// @param fileExceptionList slice
// @param azureFileExt string
// @return boolean
func isValidExtension(fileExceptionList [3]string, azureFileExt string) bool {
	for _, exceptionExt := range fileExceptionList {
		if exceptionExt == azureFileExt {
			return false // Match
		}
	}

	return true // Un-Match
}

// Sleep For X Seconds
//
// @param second integer
// @return process get delayed by X seconds
func sleepFor(second int) {
	time.Sleep(time.Duration(second) * time.Second) // X Sec Halt
}
