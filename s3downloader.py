import csv
import hashlib
import logging.config
import os
import sys
#import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import boto3
from botocore.exceptions import NoCredentialsError, ClientError, EndpointConnectionError

import atomic_counter

# Ensure the logs directory exists
logs_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(logs_dir, exist_ok=True)

current_dir = os.path.abspath(os.path.dirname(__file__))
config_file_path = os.path.join(current_dir, 'log.ini')
logging.config.fileConfig(config_file_path)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

input_csv_filename = "input_data.csv"   # Will be set by cmd-args.
downloads_dir = os.path.join(current_dir, "S3_downloads")

max_locations_to_process = 100
num_of_threads = 20
num_seconds_between_requests_in_each_thread = 5
batch_size = 500_000    # 100_000 is about 40 seconds of run time, if getting only the hash & size, for non-multipart-files.
# Put -even- 500k or more, for getting metadata, especially non-multiparts, but not more than 100_000 when downloading the files.. as it will take more minutes and things may go sideways, losing intermediate results.

should_extract_hash_and_size = True
#lock = threading.Lock()    # If we ever need to synchronize some part of the code.

output_csv_filename = "result_data.csv"   # Will be set after the "input_csv_filename", given from cmd-args.
fieldnames = ['location', 'hash', 'size', 'error']

count_successful_files = 0
futures_of_threads = []

# The S3-client will use the environment-variables:
# 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'S3_ENDPOINT' and 'S3_BUCKET'
expected_bucket = os.getenv('S3_BUCKET')

def get_s3_client():
    try:
        return boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION'),
            endpoint_url=os.getenv('S3_ENDPOINT'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        raise


def parse_s3_location(s3_location):
    # Extract the bucket_name and the object_key.
    try:
        parsed = urlparse(s3_location)
        bucket_name = parsed.netloc
        object_key = parsed.path.lstrip('/')
        if bucket_name and object_key:
            return bucket_name, object_key  # remove leading slash from the object_key.
        else:
            logger.error(f"Failed to parse S3 location: {s3_location}")
            return None, None
    except Exception as e:
        logger.error(f"Failed to parse S3 location '{s3_location}': {e}")
        return None, None


def check_s3_location_parts_for_download_only(s3_location):
    bucket_name, object_key = parse_s3_location(s3_location)
    if bucket_name is None and object_key is None:
        return None, None

    if bucket_name != expected_bucket:
        logger.error(
            f"The S3_location '{s3_location}' has an unexpected bucket: '{bucket_name}' (not the expected one: '{expected_bucket}')")
        # Even if its valid, the credentials we use are only applicable for the 'expected_bucket'.
        return None, None

    return bucket_name, object_key


def process_file_for_downloading(s3_client, s3_location, downloads_dir):
    bucket_name, object_key = check_s3_location_parts_for_download_only(s3_location)
    download_result = download_file_from_s3(s3_client, s3_location, bucket_name, object_key, downloads_dir)
    # Sleep a bit to avoid overloading the server.
    if num_seconds_between_requests_in_each_thread > 0:
        time.sleep(num_seconds_between_requests_in_each_thread)
    return None != download_result


def download_file_from_s3(s3_client, s3_location, bucket_name, object_key, downloads_dir):
    try:
        local_file_path = os.path.join(downloads_dir, object_key.split('/')[1])
        s3_client.download_file(bucket_name, object_key, local_file_path)
        logger.debug(f"Downloaded '{s3_location}'.")
        return local_file_path
    except Exception as e:
        if isinstance(e, ClientError):
            logger.error(f"Failed to download file '{s3_location}': {e.response['Error']['Message']}")
        elif isinstance(e, NoCredentialsError):
            logger.error("S3 credentials not found.")
        elif isinstance(e, EndpointConnectionError):
            logger.error(f"Connection error: {str(e)}")
        else:
            error_msg = str(e)
            if "Parameter validation failed" in error_msg:
                logger.error(f"Error when validating parameters 'bucket_name': '{bucket_name}' and 'object_key': '{object_key}'")
            else:
                logger.error(f"Unexpected error when downloading file '{s3_location}': {str(e)}")
        return None


should_check_and_switch_metadata_strategy = True
# The above boolean is used to identify multiple multipart-cases and auto-switch to direct file-download, in case there are many pf suck cases.
threashold_to_switch_metadata_strategy = 10000
should_download_file_to_get_metadata = False    # It may change to True later..
multipart_counter = atomic_counter.AtomicCounter()
regular_counter = atomic_counter.AtomicCounter()


def calculate_hash_and_size_from_file(filepath, s3_location):
    try:
        md5 = hashlib.md5() # New md5-object for this file.
        file_size = 0
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(1_048_576), b''):  # 1MB buffer_size
                # (for empty file, it will not enter the loop)
                md5.update(chunk)
                file_size += len(chunk) # The length may be less than 1MB.

        error_msg = "null"
        if file_size == 0:
            error_msg = "empty file"
            logger.warning(f"Found an {error_msg}: {s3_location}")
            md5_hash = "null"  # Ignore the hash of the empty string.
        else:
            md5_hash = md5.hexdigest()
        return md5_hash, file_size, error_msg
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Could not calculate hash and/or size of file '{filepath}': {error_msg}\n{traceback.format_exc()}")
        return "null", "null", error_msg


def download_file_and_get_metadata(s3_client, s3_location, bucket_name, object_key, downloads_dir):
    filepath = download_file_from_s3(s3_client, s3_location, bucket_name, object_key, downloads_dir)
    if filepath is None:
        if num_seconds_between_requests_in_each_thread > 0:
            sleep_for_a_bit()  # The success-or-not of the 'sleep' will not play a role in the success of the location.
        return {'location': s3_location, 'hash': 'null', 'size': 'null', 'error': 'could_not_download_file_to_get_metadata'}
    else:
        md5_hash, size, error_msg = calculate_hash_and_size_from_file(filepath, s3_location)
        try:
            os.remove(filepath)
        except Exception as e:
            logger.error(f"Failed to remove file '{filepath}': {e}")
            # Do not return "False", we got the metadata.
        return {'location': s3_location, 'hash': md5_hash, 'size': size, 'error': error_msg}


def get_metadata(s3_client, s3_location, downloads_dir):
    global should_download_file_to_get_metadata
    bucket_name, object_key = parse_s3_location(s3_location)
    error_msg = None
    if bucket_name is None and object_key is None:
        error_msg = "malformed location"
    elif bucket_name != expected_bucket:
        logger.error(
            f"The S3_location '{s3_location}' has an unexpected bucket: '{bucket_name}' (instead of: '{expected_bucket}')")
        error_msg = f"unexpected bucket: {bucket_name}"
        # Even if its valid, the credentials we use are only applicable for the 'expected_bucket'.

    if error_msg:
        return {'location': s3_location, 'hash': 'null', 'size': 'null', 'error': error_msg}

    if should_download_file_to_get_metadata:    # We have switched to "download_and_get_metadata", do that immediately.
        return download_file_and_get_metadata(s3_client, s3_location, bucket_name, object_key, downloads_dir)

    try:
        response = s3_client.head_object(Bucket= bucket_name, Key=object_key)
        error_msg = "null"
        md5_hash = response['ETag'].strip('"')
        if '-' in md5_hash:
            multipart_counter.increment()
            logger.warning(f"Found a multipart file, for which we have to download and calculate its md5Sum: {s3_location}")
            # Will increase the counter (if set) and proceed with downloading and calculating the metadata.
            # For the first <threashold> cases, we do first head and then download the file if needed
            # Afterward, if at any moment the number of multipart-files exceed the number of regulars, then for all remaining locations..
            # we will download and calculate the files, to avoid double connections (headh AND then download) for most of the files (the multiparts in that case).
            if should_check_and_switch_metadata_strategy \
                and multipart_counter.get_value() >= threashold_to_switch_metadata_strategy \
                    and multipart_counter.get_value() > (regular_counter.get_value() * 1.1):    # We want the multipart-cases to be more than 10% over the regulars.
                logger.warning(f"Reached threashold of {threashold_to_switch_metadata_strategy} locations to be multipart-files. Will proceed with immediate download and calculation for all remaining locations..")
                should_download_file_to_get_metadata = True

            return download_file_and_get_metadata(s3_client, s3_location, bucket_name, object_key, downloads_dir)
        else:
            regular_counter.increment()

        size = response['ContentLength']
        if size == "0":
            error_msg = "empty file"
            logger.warning(f"Found an {error_msg}: {s3_location}")
            md5_hash = "null"   # Ignore the hash of the empty string.

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"File '{s3_location}' has a hash of '{md5_hash}' and size of '{size}'.")

        return {'location': s3_location, 'hash': md5_hash, 'size': size, 'error': error_msg}
    except Exception as e:
        error_output_msg = ""
        if isinstance(e, ClientError):
            error_response = e.response['Error']['Message']
            if "Not Found" in error_response:  # Most common error.
                error_output_msg = "not found"
            error_msg = f"ClientError for '{s3_location}': {error_response}"
        elif isinstance(e, NoCredentialsError):
            error_msg = f"S3 credentials not found."
            error_output_msg = error_msg
        elif isinstance(e, EndpointConnectionError):
            error_msg = f"Connection error: {str(e)}"
            error_output_msg = "connection error"
        else:
            error_msg = str(e)
            if "Parameter validation failed" in error_msg:
                logger.error(f"Error when validating parameters 'bucket_name': '{bucket_name}' and 'object_key': '{object_key}'")
                error_output_msg = "parameter validation error"
            else:
                logger.error(f"Unexpected error when downloading file '{s3_location}': {str(e)}")
                error_output_msg = "unknown error"

        logger.error(error_msg)
        return {'location': s3_location, 'hash': 'null', 'size': 'null', 'error': error_output_msg}


def sleep_for_a_bit():
    try:
        time.sleep(num_seconds_between_requests_in_each_thread)
    except InterruptedError as ie:
        logger.warning(f"Sleep of {num_seconds_between_requests_in_each_thread} seconds was interrupted!")
    except Exception as e:
        logger.error(f"Unexpected error when sleeping: {str(e)}")   # Most likely if the time-to-sleep is negative.


def wait_for_results_and_write_to_output():
    global futures_of_threads, count_successful_files
    output_data = []
    try:
        for future in as_completed(futures_of_threads):
            try:
                row_result = future.result()
                if should_extract_hash_and_size:
                    output_data.append(row_result)
                if row_result['error'] == 'null':
                    count_successful_files += 1
            except Exception as e:
                logger.error(f"Caught exception: {e}\n{traceback.format_exc()}")
        futures_of_threads = []  # Reset for next batch.

        if should_extract_hash_and_size:
            # Write results to output csv.
            with open(output_csv_filename, 'a', newline='') as output_csv:
                output_writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
                output_writer.writerows(output_data)
    except Exception as e:
        logger.error(f"Caught exception: {e}\n{traceback.format_exc()}")


input_rows_to_skip = 0


def process_multiple_files_from_s3():
    start_time = time.perf_counter()

    with open(input_csv_filename, 'r', newline='') as input_csv:
        try:
            reader = csv.reader(input_csv)
        except Exception as e:
            logger.error(f"Error when reading the csv file '{input_csv_filename}': {e}\n{traceback.format_exc()}")
            return False

        s3_client = get_s3_client() # The client can be shared across threads, but not across processes.

        # For machines with many CPU cores (> 8), the "ProcessPoolExecutor" is best, otherwise the "ThreadPoolExecutor" is the right choice.
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            total_locations_count = 0
            current_batch_files_count = 0
            max_locations_reached = False

            if input_rows_to_skip > 0:
                logger.info(f"Will skip the first {input_rows_to_skip} rows..")
            skipped_rows = 0
            rows_count = 0

            for row in reader:  # Stream through the input_file.
                #logger.debug(f"row: {row}")
                rows_count += 1
                if 0 < input_rows_to_skip == rows_count:
                    skipped_rows += 1
                    continue

                s3_location = row[0]
                # logger.debug(f"s3_location: {s3_location}")
                if s3_location == "location" or s3_location == "":  # Skip the header row or empty lines.
                    skipped_rows += 1
                    continue
                elif not s3_location.startswith("s3://"):
                    logger.warning(f"Skipping non-s3_location: '{s3_location}'")
                    skipped_rows += 1
                    continue

                current_batch_files_count += 1
                total_locations_count += 1

                try:
                    if should_extract_hash_and_size:
                        futures_of_threads.append(executor.submit(get_metadata, s3_client, s3_location, downloads_dir))
                    else:
                        futures_of_threads.append(executor.submit(process_file_for_downloading, s3_client, s3_location, downloads_dir))
                except Exception as e:
                    logger.error(f"Failed to submit task for location: {s3_location}")
                    continue

                if max_locations_to_process > 0 and (total_locations_count >= max_locations_to_process):
                    max_locations_reached = True

                if max_locations_reached or current_batch_files_count >= batch_size:   # Avoid submitting too many tasks to the executors. Wait for existing to finish.
                    wait_for_results_and_write_to_output()
                    if max_locations_reached:
                        break
                    else:
                        logger.info(f"Processed {total_locations_count} locations so far..")
                        current_batch_files_count = 0 # Reset counting for next batch.

            logger.info(f"Skipped {skipped_rows} rows.")

            if not max_locations_reached:
            # If the end of input was reached before the "max_locations_reached", wait for the threads to finish and writer thh output results.
                wait_for_results_and_write_to_output()

        elapsed_time = (time.perf_counter() - start_time)
        if elapsed_time > 3600:
            time_str = f"{round(elapsed_time / 3600, 2)} hours"
        elif elapsed_time > 60:
            time_str = f"{round(elapsed_time / 60, 2)} minutes"
        else:
            time_str = f"{round(elapsed_time, 2)} seconds"
        logger.info(f"Successfully processed {count_successful_files} files (out of {total_locations_count}), in {time_str}.")
    return True


def main():
    global input_csv_filename, output_csv_filename, downloads_dir, max_locations_to_process, num_of_threads, num_seconds_between_requests_in_each_thread
    if len(sys.argv) != 6:  # The 1st arg is this script's name.
        logger.error(f"Invalid arguments-number: {len(sys.argv)}")
        print("Please give exactly 5 arguments: <csv_filename> <downloads_dir> <max_files_to_download> <num_of_threads> <num_seconds_between_requests_in_each_thread>", file=sys.stderr)
        exit(1)

    input_csv_filename = sys.argv[1]  # e.g. "input_data.csv" input-file
    if not input_csv_filename.endswith(".csv"):
        logger.error(f"Invalid input file given: {input_csv_filename}")
        print("Please provide a CSV file as input..", file=sys.stderr)
        exit(2)

    if os.sep in input_csv_filename:
        input_csv_filename_parts = input_csv_filename.split(os.sep)
        input_base_path = os.sep.join(input_csv_filename_parts[0:-1])
        output_csv_filename = os.path.join(input_base_path, f"result_{input_csv_filename_parts[-1]}")
    else:
        output_csv_filename = f"result_{input_csv_filename}"

    downloads_dir = sys.argv[2]  # e.g. "S3_downloads" directory
    if not os.path.isdir(downloads_dir):
        logger.info(f"Will create the downloads_dir: {downloads_dir}")
        try:
            os.mkdir(downloads_dir)
        except OSError as error:
            logger.error(f"Error when creating the downloads_dir: {downloads_dir}")
            print("The given download-dir (" + downloads_dir + ") could not be created!\n" + error.__str__(), file=sys.stderr)
            exit(3)

    max_locations_to_process = int(sys.argv[3])  # e.g. "1000" files
    if max_locations_to_process < 0:
        logger.error(f"Invalid 'max_files_to_download' was given: {max_locations_to_process}")
        print("Please provide a positive value (including 0) for the 2nd argument \"max_files_to_download\"!", file=sys.stderr)
        exit(4)
    elif max_locations_to_process == 0:
        logger.info("Will download all available files.")
    else:
        logger.info(f"Will download up to {max_locations_to_process} files.")

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

    process_multiple_files_from_s3()
    exit(0)


if __name__ == '__main__':
    main()
