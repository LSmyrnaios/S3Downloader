# S3Downloader

This Python script takes a CSV file as input, which contains two columns: "id" and "location".
- **id**: the id of the record.<br>
- **location**: the S3-location of the files.<br>
<br>

### Requirements:
1) Run on a Linux system, with Python3.
2) The "[**s3cmd**](https://github.com/s3tools/s3cmd)" tool must be installed and configured to use the wanted S3 Object Store.
3) Have the input CSV file ready.


### Run-instructions:
`python3 s3downloader.py <input_file.csv> <downloads_dir> <max_files_to_download>`

Notes:<br>
If we want to download all the files, then we set the "**max_files_to_download**" to **zero**. 
