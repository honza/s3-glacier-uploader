// s3-glacier-uploader --- upload large files to S3 Glacier
// Copyright (C) 2022  Honza Pokorny <honza@pokorny.ca>

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

const (
	PART_SIZE = 50 * 1024 * 1024
	RETRIES   = 2
)

// CLI flags
var BucketName string
var Region string
var UploadID string

var rootCmd = &cobra.Command{
	Use:   "s3-glacier-uploader file",
	Short: "s3-glacier-uploader",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := Upload(BucketName, Region, args[0], UploadID)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

type partUploadResult struct {
	completedPart *s3.CompletedPart
	err           error
}

func calculateMd5Digest(input []byte) string {
	return fmt.Sprintf("%x", md5.Sum(input))
}

func Upload(bucket string, region string, filename string, uploadID string) error {
	s3session := s3.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	})))

	key := path.Base(filename)

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	approximateChunkCount := (fileSize / PART_SIZE) + 1

	fmt.Println("File to upload:", filename)

	if uploadID != "" {
		return fmt.Errorf("We can't resume uploads yet.  It's on the roadmap.")
	}

	createdResp, err := s3session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		StorageClass: aws.String(s3.ObjectStorageClassDeepArchive),
	})

	if err != nil {
		return err
	}

	fmt.Println("Upload ID:", *createdResp.UploadId)

	var partNum = 1
	var completedParts []*s3.CompletedPart

	buffer := make([]byte, PART_SIZE)
	reader := bufio.NewReader(file)

	// When an object is uploaded as a multipart upload, the ETag for the object is
	// not an MD5 digest of the entire object. Amazon S3 calculates the MD5 digest
	// of each individual part as it is uploaded. The MD5 digests are used to
	// determine the ETag for the final object. Amazon S3 concatenates the bytes for
	// the MD5 digests together and then calculates the MD5 digest of these
	// concatenated values. The final step for creating the ETag is when Amazon S3
	// adds a dash with the total number of parts to the end.
	//
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
	digestBytes := []byte{}

	bar := progressbar.Default(int64(approximateChunkCount))

	for {
		n, err := reader.Read(buffer)

		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("Failed to read a chunk: %w", err)
		}

		// If we've read less than the chunk size, truncate the buffer.
		if n < PART_SIZE {
			buffer = buffer[:n]
		}

		db := md5.Sum(buffer)
		for _, b := range db {
			digestBytes = append(digestBytes, b)
		}

		result := uploadToS3(s3session, createdResp, buffer, partNum)

		if result.err != nil {
			return fmt.Errorf("Upload not aborted.  You can resume it.  Not implemented yet.  Error: %w", result.err)
		}

		completedParts = append(completedParts, result.completedPart)
		partNum++

		bar.Add(1)
	}

	etag := fmt.Sprintf("%s-%d", calculateMd5Digest(digestBytes), partNum-1)

	// Signalling AWS S3 that the multiPartUpload is finished
	resp, err := s3session.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   createdResp.Bucket,
		Key:      createdResp.Key,
		UploadId: createdResp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	if err != nil {
		return err
	}

	fmt.Println("Success!")
	respEtag := strings.Trim(*resp.ETag, "\"")

	if respEtag == etag {
		fmt.Println("Etags match!")
	} else {
		fmt.Println("Etags don't match!")
		fmt.Println("  AWS: ", respEtag)
		fmt.Println("  Ours:", etag)
	}

	fmt.Println(*resp.Location)

	return nil
}

func uploadToS3(s3session *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNum int) partUploadResult {
	var try int
	for try <= RETRIES {
		uploadRes, err := s3session.UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(fileBytes),
			Bucket:        resp.Bucket,
			Key:           resp.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      resp.UploadId,
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})

		if err != nil {
			fmt.Println(err)
			if try == RETRIES {
				return partUploadResult{nil, err}
			} else {
				try++
				time.Sleep(time.Duration(time.Second * 15))
			}
		} else {
			return partUploadResult{
				&s3.CompletedPart{
					ETag:       uploadRes.ETag,
					PartNumber: aws.Int64(int64(partNum)),
				}, nil,
			}
		}
	}

	return partUploadResult{}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&BucketName, "bucket", "", "")
	rootCmd.PersistentFlags().StringVar(&Region, "region", "us-east-1", "")
	rootCmd.PersistentFlags().StringVar(&UploadID, "upload-id", "", "")
}

func main() {
	rootCmd.Execute()
}
