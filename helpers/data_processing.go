// Namespace: helpers/data_processing.go

package helpers

import (
	"fmt"
	"math"
	"strings"
)

const (
	columnCapacity = 10
	tenGap         = 10
)

// CreateContainerMatrix - Create matrix table for Container
func CreateContainerMatrix(container []string) [][]string {
	fmt.Println("Arranging Matrix")

	// Setting Range Variable(s)
	var maxRange, minRange int

	// Total rows to be created.
	containerLength := len(container)

	// Getting capacity for slice
	rowsCapacity := int(math.Ceil(float64(containerLength) / tenGap))

	// Creating 2D container slice
	containerMatrix := make([][]string, rowsCapacity)
	for rowIdx := 0; rowIdx < rowsCapacity; rowIdx++ {
		currentDifference := math.Abs(float64(containerLength - (rowIdx * columnCapacity)))

		minRange = (rowIdx * tenGap) // Getting minimum range["min":max]
		if currentDifference >= columnCapacity {
			maxRange = (1 + rowIdx) * columnCapacity // Getting maximum range[min:"max"]
		} else {
			maxRange = minRange + int(currentDifference) // Getting maximum range[min:"max"]
		}

		containerMatrix[rowIdx] = container[minRange:maxRange] // Setting 2D container matrix
	}

	return containerMatrix
}

// ProcessBlobName - Modifying Blob Name (optional)
func ProcessBlobName(containerName string, blobName string) string {
	if strings.HasPrefix(containerName, "asset-") {
		if strings.HasSuffix(blobName, "1800kbps_AAC_und_ch2_128kbps.mp4") {
			blobName = strings.Replace(blobName, "_H264_1800kbps_AAC_und_ch2_128kbps", "-sd", -1)
		} else if strings.HasSuffix(blobName, "4500kbps_AAC_und_ch2_128kbps.mp4") {
			blobName = strings.Replace(blobName, "_H264_4500kbps_AAC_und_ch2_128kbps", "-hd", -1)
		} else {

		}
	}

	return blobName
}
