// namespace: init.go

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"./database"
	"./download/azure"
	"./sync"
	"./upload/s3"

	"github.com/joho/godotenv"
)

// EnvVars Struct
type EnvVars struct {
	accountName, accountKey                 string // Azure Specific Setting
	awsKey, awsSecret, awsBucket, awsRegion string // AWS Specific Setting
	dbName                                  string // DB Specific Setting
	mediaFolder                             string // Content Specific Setting
}

// Global Variable
var status bool

// Main Function
func main() {
	fmt.Println("[START] Sync Cloud Storage")

	// Loading .env variables
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Processing .env Configuration File.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	awsKey, awsSecret, awsBucket, awsRegion := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_BUCKET"), os.Getenv("AWS_DEFAULT_REGION")
	dbName, mediaFolder := os.Getenv("DB_FILE"), os.Getenv("MEDIA_FOLDER")
	if len(accountName) == 0 || len(accountKey) == 0 {
		log.Fatal("Azure Credentials are missing from environment variable (.env)")
	}

	if len(awsKey) == 0 || len(awsSecret) == 0 || len(awsBucket) == 0 || len(awsRegion) == 0 {
		log.Fatal("AWS Credentials are missing from environment variable (.env)")
	}

	if len(dbName) == 0 {
		log.Fatal("DB Setting is missing from environment variable (.env)")
	}

	if len(mediaFolder) == 0 {
		log.Fatal("Media Storage is missing from environment variable (.env)")
	}

	// Checking if DB File Exists
	if _, err := os.Stat(dbName); os.IsNotExist(err) {
		log.Fatal("DB File is Missing!")
	}

	// Initializing Global Flag
	syncFlag := flag.Bool("sync", false, "a bool")         // Init: Sync Flag!
	cleanFlag := flag.Bool("clean", false, "a bool")       // Init: Clean Flag!
	uploadFlag := flag.Bool("upload", false, "a bool")     // Init: Upload Flag!
	downloadFlag := flag.Bool("download", false, "a bool") // Init: Download Flag!

	// Initializing Sync Flag
	containerFlag := flag.Bool("container", false, "a bool") // Init: Container Flag!
	blobFlag := flag.Bool("blob", false, "a bool")           // Init: Container:Blob Flag!

	// Initializing Reset Flag
	resetLiveContainerFlag := flag.Bool("reset-live", false, "a bool") // Init: Reset Live Container Flag!

	flag.Parse() // Parsing the command line flag data

	// Check Flag
	if *syncFlag {
		env := sync.EnvVars{AccountName: accountName, AccountKey: accountKey, ContainerFlag: *containerFlag, BlobFlag: *blobFlag}
		if database.BuildTable() {
			status = sync.Run(env)
		} else {
			fmt.Println("Sync Table Creation Failed!")
		}
	} else if *cleanFlag {
		if !database.CleanUp() {
			fmt.Println("CleanUp Failed!")
		}
	} else if *uploadFlag {
		env := s3.EnvVars{AWSKey: awsKey, AWSSecret: awsSecret, AWSBucket: awsBucket, AWSRegion: awsRegion, MediaFolder: mediaFolder}
		status = s3.Run(env)
	} else if *downloadFlag {
		env := azure.EnvVars{AccountName: accountName, AccountKey: accountKey, MediaFolder: mediaFolder}
		status = azure.Run(env)
	} else if *resetLiveContainerFlag {
		database.ResetLiveContainer()
	} else {
		fmt.Println("Invalid Flag")
	}

	fmt.Println("[END] Sync Cloud Storage")
}
