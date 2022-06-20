import json
import logging
from typing import List, Set

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.classes import APIDownloader, Generation, EndpointsType

SNOWPIPE_FILES_FOLDER = Variable.get("snowpipe_files") # "{{ var.value.snowpipe_files }}"
INDIVIDUAL_FOLDER = "Antropova/"
INDIVIDUAL_SNOWPIPE_FILES_FOLDER = SNOWPIPE_FILES_FOLDER + INDIVIDUAL_FOLDER

BUCKET, FOLDER_KEY = S3Hook.parse_s3_url(INDIVIDUAL_SNOWPIPE_FILES_FOLDER)

s3hook = S3Hook()

logging.basicConfig(
    format="%(asctime)s => %(message)s",
    level=logging.DEBUG
)


def _print(message: str) -> None:
    """
    Prints out the message into an Airflow log
    """
    print(message)


def _fetch_api(endpoint: EndpointsType, s3_key: str) -> None:
    """
    Downloads via API json data for all the entities of a
    given endpoint, filters it and uploads the filtered data
    as a json file into an s3 bucket.
    """
    downloader = APIDownloader(endpoint)
    filtered_data = downloader.download_and_filter_all_entities()
    str_data = json.dumps(filtered_data)
    s3hook.load_string(string_data=str_data, key=s3_key, replace=True)


def get_generations_from_api() -> Set[int]:
    """
    Returns a set of generation ids from API
    """
    downloader = APIDownloader(Generation)
    return downloader.get_ids_to_load()


def list_generation_keys() -> List[str]:
    """
    Returns a list of S3 URIs of files containing generation data
    within s3_prefix folder
    """
    keys = s3hook.list_keys(bucket_name=BUCKET, prefix=FOLDER_KEY, delimiter='/')
    return [key for key in keys if "generation" in key]


def parse_generations_from_s3(file: str) -> Set[int]:
    """
    Reads in the string data about generation ids from s3,
    parses it and returns a set of generation ids
    """
    str_data = s3hook.read_key(file)
    return set(map(int, str_data.split(", ")))


def get_latest_generations_from_s3():
    """
    Returns a set of generation ids from the latest generation
    file in a given s3 folder.
    Returns None if there is no such file.
    """
    generation_keys = list_generation_keys()
    if not generation_keys:
        return None
    last_generation_key = max(generation_keys)
    return parse_generations_from_s3(f"s3://{BUCKET}/{last_generation_key}")


def _check_generations(date) -> None:
    """
    Downloads a set of generation ids from API, compares it to
    the latest data from s3 and if any changes are discovered
    writes the actual API data to a new s3 file.
    """
    api_generation_ids = get_generations_from_api()
    s3_generation_ids = get_latest_generations_from_s3()

    if s3_generation_ids is not None and s3_generation_ids == api_generation_ids:
        logging.info("There are no changes in generation ids from the Pokemon API")
        return

    str_data = ", ".join(map(str, api_generation_ids))
    s3_key = f"{INDIVIDUAL_SNOWPIPE_FILES_FOLDER}generations_{date}.txt"
    s3hook.load_string(string_data=str_data, key=s3_key)
    logging.info(f"The current version of generation ids from the Pokemon API is loaded to {s3_key}")

