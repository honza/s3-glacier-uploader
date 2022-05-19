s3-glacier-uploader
===================

A CLI tool for uploading large files to AWS S3 Glacier

We assume that you have the AWS tools configured, and we use that for
authentication.  When uploading a file, you need to give us a bucket name, and
a region.  We set the storage class to "Deep Archive".

## Usage

To upload a file:

```
$ s3-glacier-uploader --bucket <bucket name> --region <AWS region> <file>
```

Your file is uploaded in 50MB chunks, and can be really big.  AWS produces an MD5
checksum for each chunk so we verify the integrity of the data.

## TODO

* Resuming a failed upload

## Prior art

Originally based on

https://mehranjnf.medium.com/s3-multipart-upload-with-goroutines-92a7aebe831b
