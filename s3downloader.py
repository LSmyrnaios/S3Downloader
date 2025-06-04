import csv
import hashlib
import logging.config
import os
import subprocess
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure the logs directory exists
logs_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(logs_dir, exist_ok=True)

config_file_path = os.path.join(os.path.dirname(__file__), 'log.ini')
logging.config.fileConfig(config_file_path)
logger = logging.getLogger(__name__)

input_csv_filename = "input_data.csv"
downloads_dir = "S3_downloads"

max_files_to_download = 100
num_of_threads = 20
num_seconds_between_requests_in_each_thread = 5
batch_size = 10000

should_extract_hash_and_size = True
should_delete_file_after_calculation = True
output_data = []
output_csv_filename = "output_hash_size.csv"
fieldnames = ['location', 'hash', 'size', 'error']

count_successful_files = 0
futures_of_threads = []


def get_md5_and_size(filepath):
    try:
        md5 = hashlib.md5() # New md5-object for this file.
        file_size = 0
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(1_048_576), b''):  # 1MB buffer_size
                md5.update(chunk)
                file_size += len(chunk) # The length may be less than 1MB.

        if file_size == 0:
             logger.warning(f"An empty file was found: {filepath}")
        #     return "null", 0  # In case we do not want to calculate the hash of empty files.
        return md5.hexdigest(), file_size
    except Exception as e:
        logger.error(f"Could not calculate hash and/or size of file '{filepath}': {e}\n{traceback.format_exc()}")
        return None, None


def download_file_from_s3(s3_file_location):
    global downloads_dir, num_seconds_between_requests_in_each_thread, output_data
    result = None
    try:
        result = subprocess.check_output("s3cmd get " + s3_file_location + " " + downloads_dir, shell=True,
                                         executable="/bin/bash", stderr=subprocess.STDOUT)

        if should_extract_hash_and_size:
            filepath = os.path.join(downloads_dir, os.path.basename(s3_file_location))
            file_hash, file_size = get_md5_and_size(filepath)
            if file_hash and file_size:  # in case of an exception both will be None, otherwise we want
                logger.info(f"Downloaded file '{filepath}' with hash '{file_hash}' and size: {file_size}")
                output_data.append({
                    'location': s3_file_location,
                    'hash': file_hash,
                    'size': file_size,
                    'error': "null"
                })
            else:
                logger.error(f"Failed to extract hash ({file_hash}) and size ({file_size})!")
                output_data.append({
                    'location': s3_file_location,
                    'hash': 'null',
                    'size': 'null',
                    'error': "hash and size calculation problem"
                })
                return False
            if should_delete_file_after_calculation:
                os.remove(filepath)
        return True
    except subprocess.CalledProcessError as cpe:
        if cpe.returncode in [2, 3, 4, 5, 6, 10, 11]:
            logger.error(f"Serious S3 error: {cpe}\nExiting the main process..")
            exit(99)

        result = cpe.output
        if cpe.returncode == 12:
            logger.error(f"File not found: {s3_file_location}")
            if should_extract_hash_and_size:
                output_data.append({
                    'location': s3_file_location,
                    'hash': 'null',
                    'size': 'null',
                    'error': "location not found"
                })
        else:
            logger.error(f"Failed to download file '{s3_file_location}': {cpe}")
            if result is not None and should_extract_hash_and_size:
                output_data.append({
                    'location': s3_file_location,
                    'hash': 'null',
                    'size': 'null',
                    'error': result.replace(",", ";")   # Make sure it's a proper csv column
                })
            elif should_extract_hash_and_size:
                output_data.append({
                    'location': s3_file_location,
                    'hash': 'null',
                    'size': 'null',
                    'error': f's3cmd {cpe.returncode} code'   # Make sure it's a proper csv column
                })
        return False
    except Exception as e:
        logger.error(f"Failed to download file '{s3_file_location}': {e}\n{traceback.format_exc()}")
        if should_extract_hash_and_size:
            output_data.append({
                'location': s3_file_location,
                'hash': 'null',
                'size': 'null',
                'error': str(e).replace(",", ";")    # Make sure it's a proper csv column
            })
        return False
    finally:
        if logger.isEnabledFor(logging.DEBUG) and result is not None:
            strings = []
            for line in result.splitlines():
                decoded_line = line.decode("utf-8")
                if "%" in decoded_line and not "100%" in decoded_line:  # Do not show intermediate progress of individual files, in the logs.
                    continue
                strings.append(decoded_line)
            logger.debug(''.join(strings))

        # Sleep a bit to avoid overloading the server.
        if num_seconds_between_requests_in_each_thread > 0:
            time.sleep(num_seconds_between_requests_in_each_thread)


def wait_for_results_and_write_to_output():
    global futures_of_threads, count_successful_files, output_data
    for future in as_completed(futures_of_threads):
        try:
            if future.result():
                count_successful_files += 1
        except Exception as e:
            logger.error(f"Caught exception: {e}\n{traceback.format_exc()}")
    futures_of_threads = []  # Reset for next batch.

    if should_extract_hash_and_size:
        # Write results to output csv.
        with open(output_csv_filename, 'a', newline='') as output_csv:
            output_writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
            output_writer.writerows(output_data)
        output_data = []  # Reset for next batch.


def download_multiple_files_from_s3():
    global max_files_to_download, num_of_threads, output_data, input_csv_filename

    start_time = time.perf_counter()

    with open(input_csv_filename, 'r', newline='') as input_csv:
        try:
            reader = csv.reader(input_csv)
        except Exception as e:
            logger.error(f"Error when reading the csv file '{input_csv_filename}': {e}\n{traceback.format_exc()}")
            return False

        # For machines with many CPU cores (> 8), the "ProcessPoolExecutor" is best, otherwise the "ThreadPoolExecutor" is the right choice.
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            total_files_count = 0
            current_batch_files_count = 0
            max_files_reached = False

            for row in reader:  # Stream through the input_file.
                #logger.debug(f"row: {row}")
                file_location = row[0]
                # print("file_location: " + file_location)
                if file_location == "location":    # This is the header row.
                    continue

                current_batch_files_count += 1
                total_files_count += 1

                try:
                    futures_of_threads.append(executor.submit(download_file_from_s3, file_location))
                except Exception as e:
                    logger.error(f"Failed to submit task for location: {file_location}")
                    continue

                if max_files_to_download > 0 and (total_files_count >= max_files_to_download):
                    max_files_reached = True

                if max_files_reached or current_batch_files_count >= batch_size:   # Avoid submitting too many tasks to the executors. Wait for existing to finish.
                    wait_for_results_and_write_to_output()
                    if max_files_reached:
                        break
                    else:
                        logger.info(f"Processed {total_files_count} locations so far..")
                        current_batch_files_count = 0 # Reset counting for next batch.

            if not max_files_reached:
            # If the end of input was reached before the "max_files_reached", wait for the threads to finish and writer thh output results.
                wait_for_results_and_write_to_output()

        logger.info(f"Successfully processed {count_successful_files} files (out of {total_files_count}), in {(time.perf_counter() - start_time)} seconds.")
    return True


def main():
    global input_csv_filename, downloads_dir, max_files_to_download, num_of_threads, num_seconds_between_requests_in_each_thread
    if len(sys.argv) != 6:  # The 1st arg is this script's name.
        logger.error(f"Invalid arguments-number: {len(sys.argv)}")
        print("Please give exactly 5 arguments: <csv_filename> <downloads_dir> <max_files_to_download> <num_of_threads> <num_seconds_between_requests_in_each_thread>", file=sys.stderr)
        exit(1)

    input_csv_filename = sys.argv[1]  # e.g. "input_data.csv" input-file
    if not input_csv_filename.endswith(".csv"):
        logger.error(f"Invalid input file given: {input_csv_filename}")
        print("Please provide a CSV file as input..", file=sys.stderr)
        exit(2)

    downloads_dir = sys.argv[2]  # e.g. "S3_downloads" directory
    if not os.path.isdir(downloads_dir):
        logger.info(f"Will create the downloads_dir: {downloads_dir}")
        try:
            os.mkdir(downloads_dir)
        except OSError as error:
            logger.error(f"Error when creating the downloads_dir: {downloads_dir}")
            print("The given download-dir (" + downloads_dir + ") could not be created!\n" + error.__str__(), file=sys.stderr)
            exit(3)

    max_files_to_download = int(sys.argv[3])  # e.g. "1000" files
    if max_files_to_download < 0:
        logger.error(f"Invalid 'max_files_to_download' was given: {max_files_to_download}")
        print("Please provide a positive value (including 0) for the 2nd argument \"max_files_to_download\"!", file=sys.stderr)
        exit(4)
    elif max_files_to_download == 0:
        logger.info("Will download all available files.")
    else:
        logger.info(f"Will download up to {max_files_to_download} files.")

    num_of_threads = int(sys.argv[4])  # e.g. "20" threads
    if num_of_threads <= 0:
        logger.error(f"Invalid 'num_of_threads' was given: {num_of_threads}")
        print("Please provide an above-zero value for the 3rd argument \"num_of_threads\"!", file=sys.stderr)
        exit(5)
    else:
        logger.info(f"Will download the files using {num_of_threads} threads.")

    num_seconds_between_requests_in_each_thread = int(sys.argv[5])  # e.g. "5" seconds
    if num_seconds_between_requests_in_each_thread < 0:
        logger.error(f"Invalid 'num_seconds_between_requests_in_each_thread' was given: {num_seconds_between_requests_in_each_thread}")
        print("Please provide a positive value (including 0) for the 4th argument \"num_seconds_between_requests_in_each_thread\"!", file=sys.stderr)
        exit(6)
    elif num_seconds_between_requests_in_each_thread == 0:
        logger.info("Will download the files with 0 sleep between requests.")
    else:
        logger.info(f"Will apply a sleep between requests of {num_seconds_between_requests_in_each_thread} seconds.")

    if should_extract_hash_and_size:
        logger.info(f"Will (re)create the output file '{output_csv_filename}'.")
        with open(output_csv_filename, 'w', newline='') as output_csv:
            writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
            writer.writeheader()

    download_multiple_files_from_s3()
    exit(0)


if __name__ == '__main__':
    main()
