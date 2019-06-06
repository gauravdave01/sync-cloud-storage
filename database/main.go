// Namespace: database/main.go

package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite3 Connection
)

// Global Variable(s)
var dbConnection *sql.DB
var liveContainerList = []string{
	"employees-data", "reports"} // Containers which are live and gets updated frequently

// Handling Error
func handleDBErrors(err error, reason string) {
	if err != nil {
		log.Println("DB Issue: ", reason)
		log.Fatal(err)
	}
}

// InitConnection - Initialize DB Connection
func InitConnection() *sql.DB {
	// Initializing DB Connection
	fmt.Println("Initializing DB Connection...")

	// Establishing SQLite Connection
	dbConnection, _ = sql.Open("sqlite3", "./storage.sqlite?cache=shared&mode=rwc")

	// Returns DB Connection
	return dbConnection
}

// CloseConnection - Close DB Connection
func CloseConnection() {
	// Closing DB Connection
	fmt.Println("Closing DB Connection...")
	dbConnection.Close()
}

// CleanUp - Drops/Delete the "older" table and creates afresh.
func CleanUp() bool {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	fmt.Println("-------------")
	fmt.Println("DB: CleanUP!")
	fmt.Println("-------------")

	_, containerErr := dbConnection.Exec("DROP TABLE IF EXISTS containers") // Drop "container" Table
	_, syncErr := dbConnection.Exec("DROP TABLE IF EXISTS sync")            // Drop "sync" Table

	if containerErr != nil || syncErr != nil {
		return false
	}

	fmt.Println("Deleted Table(s): containers, sync.")
	return true // Success
}

// BuildTable - Create sync table
func BuildTable() bool {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	// Checking Container Table
	fmt.Println("Checking Container Table...")

	// Creating Container Table (If Not Exists)
	createContainer, _ := dbConnection.Prepare(`
		CREATE TABLE IF NOT EXISTS containers (
			name TEXT UNIQUE,
			status INTEGER DEFAULT 0,
			created_at TEXT
	)`)
	_, containerErr := createContainer.Exec()

	// Checking if Error occurred
	if containerErr != nil {
		return false
	}

	// Checking Sync Table
	fmt.Println("Checking Sync Table...")

	// Creating Sync Table (If Not Exists)
	createSync, _ := dbConnection.Prepare(`
		CREATE TABLE IF NOT EXISTS sync (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			container TEXT NOT NULL,
			blob TEXT,
			azure_status INTEGER DEFAULT 0,
			azure_error TEXT,
			s3_status INTEGER DEFAULT 0,
			s3_error TEXT,
			created_at TEXT,
			updated_at TEXT
	)`)
	_, syncErr := createSync.Exec()

	// Checking if Error occurred
	if syncErr != nil {
		return false
	}

	return true // Success
}

// InsertInContainer - Insert new entry into the containers table
func InsertInContainer(container string) bool {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	// Preparing Statement
	insertStatement, statementError := dbConnection.Prepare("INSERT or IGNORE INTO containers(name, created_at) values(?,?)")
	handleDBErrors(statementError, "Insert Container Prepare Failed")

	// Executing Statement
	insertResponse, insertError := insertStatement.Exec(container, time.Now().Local())
	handleDBErrors(insertError, "Insert Container  Execute Failed")

	// Printing Last Inserted ID
	lastID, _ := insertResponse.LastInsertId()
	fmt.Println("DB ID:", lastID)

	return true // Success
}

// GetPendingAzureContent - Get container:blob mapping with pending download from Microsoft Azure
func GetPendingAzureContent() map[int]map[string]string {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	var syncList = map[int]map[string]string{}

	// Testing:
	// syncRows, syncErr := dbConnection.Query("SELECT container, blob FROM sync WHERE azure_status = 0 and id in (1, 10, 11)")
	// syncRows, syncErr := dbConnection.Query("SELECT container, blob FROM sync WHERE azure_status = ? AND container = ? AND blob = ? LIMIT 10", 0, "images", "test-image-1.jpg")
	// syncRows, syncErr := dbConnection.Query("SELECT container, blob FROM sync WHERE azure_status = ? AND id = ?", 0, 1001)

	// Fetching 10 Eligible Entries
	syncRows, syncErr := dbConnection.Query("SELECT container, blob FROM sync WHERE azure_status = ? LIMIT 10", 0)
	handleDBErrors(syncErr, "[Azure] Select Container:Blobs Failed")

	defer syncRows.Close() // Closing Row Pointer

	idx := 0
	for syncRows.Next() {
		var container, blob string
		syncLoopErr := syncRows.Scan(&container, &blob)

		if syncLoopErr != nil {
			handleDBErrors(syncLoopErr, "[Azure] Select Container:Blob Mapping Failed")
		}

		syncList[idx] = map[string]string{}
		syncList[idx]["container"] = container
		syncList[idx]["blob"] = blob
		idx++
	}

	// Any error encountered during iteration
	loopError := syncRows.Err()
	handleDBErrors(loopError, "[Azure] Container:Blob Iteration Failed")

	return syncList
}

// GetPendingS3Content - Get container:blob mapping with pending upload to Amazon S3
func GetPendingS3Content() map[int]map[string]string {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	var syncList = map[int]map[string]string{}

	// Fetching 10 Eligible Entries
	syncRows, syncErr := dbConnection.Query("SELECT container, blob FROM sync WHERE azure_status = ? AND s3_status = ? order by id desc LIMIT 10", 1, 0)
	handleDBErrors(syncErr, "[S3] Select Container:Blobs Failed")

	defer syncRows.Close() // Closing Row Pointer

	idx := 0
	for syncRows.Next() {
		var container, blob string
		syncLoopErr := syncRows.Scan(&container, &blob)

		if syncLoopErr != nil {
			handleDBErrors(syncLoopErr, "[S3] Select Container:Blob Mapping Failed")
		}

		syncList[idx] = map[string]string{}
		syncList[idx]["container"] = container
		syncList[idx]["blob"] = blob
		idx++
	}

	// Any error encountered during iteration
	loopError := syncRows.Err()
	handleDBErrors(loopError, "[S3] Container:Blob Iteration Failed")

	return syncList
}

// GetPendingContainer - Get container with pending download
func GetPendingContainer() []string {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	// Initializing Container
	var containerList []string

	// Fetching 100 Eligible Entries
	containerRows, containerErr := dbConnection.Query("SELECT name FROM containers WHERE status = ? LIMIT 100", 0)
	handleDBErrors(containerErr, "Select Containers Failed")

	defer containerRows.Close() // Closing Row Pointer

	// Looping through the Result Set
	for containerRows.Next() {
		var name string
		containerLoopErr := containerRows.Scan(&name)
		if containerLoopErr != nil {
			handleDBErrors(containerLoopErr, "Select Containers Failed")
		}
		containerList = append(containerList, name)
	}

	// Any error encountered during iteration
	loopError := containerRows.Err()
	handleDBErrors(loopError, "Container Iteration Failed")

	return containerList
}

// ResetLiveContainer - Reset Container to 0 with live status
func ResetLiveContainer() {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	for _, liveContainer := range liveContainerList {
		fmt.Println(liveContainer)
		updateContainerQuery, _ := dbConnection.Prepare("UPDATE containers SET status = ? WHERE name = ?")
		updateContainerQuery.Exec(0, liveContainer)

		fmt.Println("[", liveContainer, "] Reset Completed!")
	}
}

// SetAzureFlag - Set Flag in Sync Table w.r.t. Azure
func SetAzureFlag(containerName string, blobName string, statusCode int, errorMessage string) {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	updateSyncQuery, _ := dbConnection.Prepare("UPDATE sync SET azure_status = ?, azure_error = ?, updated_at = ? WHERE container = ? AND blob = ?")
	updateSyncQuery.Exec(statusCode, errorMessage, time.Now().Local(), containerName, blobName)
	fmt.Println("[Table: sync] Assigned Status Flag.")
}

// SetS3Flag - Set Flag in Sync Table w.r.t. S3
func SetS3Flag(containerName string, blobName string, statusCode int, errorMessage string) {
	dbConnection := InitConnection() // Create DB Conection
	defer CloseConnection()          // Close DB Connection

	updateSyncQuery, _ := dbConnection.Prepare("UPDATE sync SET s3_status = ?, s3_error = ?, updated_at = ? WHERE container = ? AND blob = ?")
	updateSyncQuery.Exec(statusCode, errorMessage, time.Now().Local(), containerName, blobName)
	fmt.Println("[Table: sync] S3: Assigned Status Flag.")
}
