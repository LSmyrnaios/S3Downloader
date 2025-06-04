# S3Downloader

This Python script takes a CSV file as input, which contains the column "location", as its first one.<br>
- **location**: the S3-location of the file<br>

Then, it downloads the files in parallel.
<br>
In case the field "should_extract_hash_and_size" is set to "True" inside the program, then the hash and size of each file is calculated and returned in an output csv-file.
<br>
In case the field "should_delete_file_after_calculation" is set the "True", then any downloaded file will be deleted right after its hash and size has been calculated.
<br>
Note: in case the files where uploaded in S3 in one part and not "multipart", then it would be possible to extract the md5hash and size of the file form the "etag", without downloading it. 
However, that would require additional permission, including the "list-bucket" one.
<br>

### Requirements:
1) Run on a Linux system, with Python3.
2) The "[**s3cmd**](https://github.com/s3tools/s3cmd)" tool must be installed and configured to use the wanted S3 Object Store.
3) Have the input CSV file ready.


### Run-instructions:
`python3 s3downloader.py <csv_filename> <downloads_dir> <max_files_to_download> <num_of_threads> <num_seconds_between_requests_in_each_thread>`

Notes:<br>
- If you want to download all the files, then set the "**max_files_to_download**" argument, to **zero** (0).
- After running experiments, it seems that the number of 64 threads is optimal for an 8-cores CPU.
