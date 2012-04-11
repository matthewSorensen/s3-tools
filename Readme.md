# s3sync

Simple tool to sync git repositories with s3 buckets. Two modes of operation: 
*--reset*, which resets the working tree and uploads every tracked file, and normal mode,
which uploads/deletes every file changed in both the index and working tree. 
S3sync can be configured to commit changes upon upload, with the *--commit* 
option, which takes a commit message.

Data is gzip'ed for upload, and the appropriate headers are set. Mime-types are guessed,
and browser caches expire in six hours.

AWS credentials are obtained from the *AWS_ACCESS_KEY*, *AWS_SECRET_KEY*, and *AWS_S3_BUCKET*
environment variables.
