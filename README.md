# Sync Cloud Storage

This repository includes files, which will syncs Azure latest uploaded files and downloads them to NAS Server. Later that data will be transferred to Amazon Snowball (and later to [Amazon S3] Bucket).

Once the data is transferred to [Snowball], there's still new content which gets uploaded to Azure, as the revamped website will be in-active. Hence, a separate sync mechanism will be used to download Azure content and gets uploaded to S3.

### Technology
I'm using [Golang] to implement this task.

---
> Algorithm(s):
---
**Algorithm #1: Sync Azure Content**
**Timeline: Till Launch**

1. Get entire list of Azure container(s)
2. Store these list in [SQLite] DB (inside **containers** table)
3. Taking **containers** as reference traverse blob and store that in **sync** table
4. Table Structure (containers): name, status
5. Table Structure (sync): id, container, blob, azure_status, azure_error, aws_status, aws_error
---
**Algorithm #2: Download Azure Content**
**Timeline: Before Snowball Transfer**

1. Fetch 10 eligible entries from **containers** *(As of now I'm taking only 10 entries, can be increased if we meet the network performance and error handling)*
2. Create 10 Go Routine to initiate 10 concurrent downloads
3. Go Routine response gets updated with azure_status 1 / 2 (+azure_error) on success / failure respectively
---
**Algorithm #3: Download Azure Content and Upload to Amazon S3**
**Timeline: After Snowball Transfer**

1. Execute the "Sync Azure Content" algorithm to fetch any new assets 
2. Execute the "Download Azure Content" algorithm to download latest content
3. Taking reference from "Download Azure Content" algorithm, upload content to Amazon S3
4. Go Routine response gets updated with s3_status 2 or 3 (+s3_error) on success and failure respectively
---
### Container Table Structure
| name | status |
| ------ | ------ |
| asset-fff8f790-7048-4556-b0c1-02877f6d7c52 | 0 |
| asset-fff50db0-48d9-4b14-9340-d96741951a91 | 0 |
---
> Note:
> Status Code (0: InActive, 100: Live, 200: Success, 404: Container Not Found, 503: Blob Not Found)

### Sync Table Structure:
| id | container | blob | azure_status | azure_error | s3_status | s3_error|
| ------ | ------ | ------ | ------ | ------ | ------ |------ |
| 1 |  asset-0019dde0-ded0-47s1-a00d-86038c7be78a | 102336-5333da2bde123309c900c0784f64b73569bba04-02461720_H264_1800kbps_AAC_und_ch2_128kbps.mp4 | 0 | nil | 0 | nil |
| 2 |  asset-00276767-9f74-476f-a163-7b135654d8d8 | 25480-2bfd5e6efa2a040dcd5eece657e0fd14bf739883-49301826_H264_4500kbps_AAC_und_ch2_128kbps.mp4 | 0 | nil | 0 | nil |
| 3 |  asset-0024r6783-116b-42at-9564-ac75da05886c | 33551-40d31ae52f90324d9889af3e3f74b65148379450-49201201_H264_4500kbps_AAC_und_ch2_128kbps.mp4 | 0 | nil | 0 | nil |
| ... | ... | ... | ... | ... | ... | ... |
---
> Note:
> Status Code(0: InActive 1: Success 2: Failure)

### Code Execution

### To run sync script:
```sh
$ cd sync-cloud-storage
$ go run init.go -sync -container
```

```sh
$ cd sync-cloud-storage
$ go run init.go -sync -blob
```

#### To reset live containers:
```sh
$ cd sync-cloud-storage
$ go run init.go -reset-live
```

#### To delete older data:
```sh
$ cd sync-cloud-storage
$ go run init.go  -clean
```

### To run azure download script:
```sh
$ cd sync-cloud-storage
$ go run init.go -download
```
### To run s3 upload script:
```sh
$ cd sync-cloud-storage
$ go run init.go -upload
```

### TODO: To cross-check uploaded content:
```sh
$ cd sync-cloud-storage
$ go run init.go -xcheck
```

[Golang]: https://golang.org
[Snowball]: https://aws.amazon.com/snowball
[Amazon S3]: https://aws.amazon.com/s3/
[SQLite]: https://www.sqlite.org/index.html
