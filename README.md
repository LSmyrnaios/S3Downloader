# S3Downloader

This Python script takes a CSV file as input, which contains two columns: "id" and "location".<br>
- **id**: the id of the record<br>
- **location**: the S3-location of the file<br>

Then, it downloads the files in parallel.
<br>

### Requirements:
1) Run on a Linux system, with Python3.
2) The "[**s3cmd**](https://github.com/s3tools/s3cmd)" tool must be installed and configured to use the wanted S3 Object Store.
3) Have the input CSV file ready.


### Run-instructions:
`python3 s3downloader.py <csv_filename> <downloads_dir> <max_files_to_download> <num_of_threads> <num_seconds_between_requests_in_each_thread>`

Notes:<br>
If you want to download all the files, then set the "**max_files_to_download**" argument, to **zero** (0). 
