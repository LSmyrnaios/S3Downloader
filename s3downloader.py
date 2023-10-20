import csv
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

csv_filename = "input_data.csv"
downloads_dir = "S3_downloads"
max_files_to_download = 100


def download_file_from_s3(s3_file_location):
    global downloads_dir
    result = None
    try:
        result = subprocess.check_output("s3cmd get " + s3_file_location + " " + downloads_dir, shell=True,
                                         executable="/bin/bash", stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError as cpe:
        result = cpe.output
        return False
    finally:
        if result is not None:
            for line in result.splitlines():
                print(line.decode())


def download_multiple_files_from_s3(csv_filename):

    futures = []

    start_time = time.perf_counter()

    with open(csv_filename, 'r') as csvfile:
        try:
            reader = csv.reader(csvfile)
        except Exception as e:
            print("Error when reading the csv file \"" + csv_filename + "\"!\n" + e.__str__(), file=sys.stderr)
            return False

        # For machines with multiple CPU cores, the "ProcessPoolExecutor" is best, otherwise the "ThreadPoolExecutor" is the right choice.
        with ThreadPoolExecutor(max_workers=20) as executor:

            count_files = 0

            for row in reader:
                if row[0] == "id":
                    continue

                # print("row: " + row.__str__())
                file_location = row[1]
                # print("file_location: " + file_location)
                futures.append(executor.submit(download_file_from_s3, file_location))

                count_files += 1
                if max_files_to_download > 0 and count_files == max_files_to_download:
                    break

        count_successful_files = 0
        for future in futures:
            if future.result():
                count_successful_files += 1

        print("Finished downloading " + count_successful_files.__str__() + " files (out of " + count_files.__str__()
              + "), in " + (time.perf_counter() - start_time).__str__() + " seconds.")
    return True


if __name__ == '__main__':

    if len(sys.argv) != 4:  # The 1st arg is this script's name.
        print("Please give exactly 3 arguments: <csv_filename> <downloads_dir> <max_files_to_download>", file=sys.stderr)
        exit(1)

    csv_filename = sys.argv[1]  # e.g. "input_data.csv"
    if not csv_filename.endswith(".csv"):
        print("Please provide a CSV file..", file=sys.stderr)
        exit(2)

    downloads_dir = sys.argv[2]  # e.g. "S3_downloads"
    if not os.path.isdir(downloads_dir):
        print("Will create the downloads_dir: " + downloads_dir.__str__())
        try:
            os.mkdir(downloads_dir)
        except OSError as error:
            print("The given download-dir (" + downloads_dir + ") could not be created!\n" + error.__str__(), file=sys.stderr)
            exit(3)

    max_files_to_download = int(sys.argv[3])  # e.g. "1000"
    if max_files_to_download < 0:
        print("Please provide a positive value for \"max_files_to_download\"!", file=sys.stderr)
        exit(4)
    elif max_files_to_download == 0:
        print("Will download all available files.")
    else:
        print("Will download up to " + max_files_to_download.__str__() + " files.")

    download_multiple_files_from_s3(csv_filename)
    exit(0)
