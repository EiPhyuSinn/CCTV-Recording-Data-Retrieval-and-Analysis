import os
from dotenv import load_dotenv
import requests
import json
import pandas as pd
import math
from datetime import datetime, timedelta
from pymongo import MongoClient
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)


def lambda_handler(event, context):
    # Dictionary mapping region to list of branches
    region_branches = {
        'Region - 1': ['DDYE', 'EIME', 'HTDA', 'KGDT', 'KYLT', 'KYME', 'LBTA', 'MUBN', 'MYAG', 'MYMA', 'PTNW', 'PYPN', 'ZALN'],
        'Region - 2': ['GWAA', 'HLGU', 'HMBI', 'KYTN', 'KHMU', 'SBTG', 'THDE', 'THLN', 'THPU'],
        'Region - 3': ['AUPN', 'KETG', 'LASK', 'LOLN', 'NASG', 'NAHO', 'PDYA', 'PILG', 'SHNG', 'TAGI'],
        'Region - 4': ['AULN', 'MAGY', 'SALN', 'TDGI'],
        'Region - 5': ['BHMO', 'KALE', 'KBLU', 'MKNA', 'MOHN', 'MOYA', 'SAGN'],
        'Region - 6': ['KPDG', 'MAHG', 'MDYA', 'MOGE', 'MTLA', 'MYGN', 'MYTA', 'POLN', 'PYBE', 'WUDN', 'YMTN'],
        'Region - 7': ['BILN', 'CHZN', 'DWEI', 'KATG', 'KYHO', 'MLME', 'MUDN', 'MYIK', 'PALW', 'PAUG', 'TBZY', 'THTN', 'YAYY'],
        'Region - 8': ['HPAN', 'LEWE', 'PHYU', 'PYMN', 'TAGO', 'TAKN', 'YDSE', 'YEGI'],
    }

    # Function to determine the region of a branch
    def get_region(branch):
        for region, branches in region_branches.items():
            if branch in branches:
                return region
        return None

    # Load environment variables from .env file (For local testing)
    env_file_name = 'Region2'
    load_dotenv(f'../apis/{env_file_name}.env')
    logging.info("Environment variables loaded.")

    # Set the MongoDB connection string as an environment variable
    os.environ['MONGODB_URL_STRING'] = 'mongodb://hanamongo:LDi4JdS!053@ec2-18-140-217-49.ap-southeast-1.compute.amazonaws.com:27017/reports?authSource=admin'
    logging.info("MongoDB connection string set.")

    # Initialize global variables
    credentials_cache = {}  # Dictionary to store credentials for different APIs
    logging.info("Global variables initialized.")

    def load_cache(file_name):
        """Function to load cache from JSON file"""
        try:
            with open(file_name, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return {}

    def save_cache(cache, file_name):
        """Function to save cache to JSON file"""
        with open(file_name, 'w') as file:
            json.dump(cache, file)

    def get_mongo_connection():
        """Function to establish connection to MongoDB"""
        mongo_url = os.environ.get('MONGODB_URL_STRING')
        if mongo_url:
            client = MongoClient(mongo_url)
            return client
        else:
            logging.error("MongoDB connection string not found.")
            return None


    def insert_to_mongodb(data, collection_name):
        client = get_mongo_connection()
        if client:
            db = client.get_database()
            collection = db[collection_name]
            try:
                for recording in data:
                    event_id = recording['eventId']
                    # Set created_at and updated_at fields
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    recording['created_at'] = current_time
                    recording['updated_at'] = current_time
                    # Check if the event ID already exists in the collection
                    existing_record = collection.find_one({'eventId': event_id})
                    if existing_record:
                        # If the document exists, update it
                        recording['updated_at'] = current_time
                        collection.update_one({'eventId': event_id}, {'$set': recording})
                        logging.info(f"Updated document for event ID {event_id}")
                    else:
                        # If the document does not exist, insert it
                        collection.insert_one(recording)
                        logging.info(f"Inserted new document for event ID {event_id}")
            except Exception as e:
                logging.error(f"Error inserting to MongoDB: {e}")
            finally:
                client.close()
        else:
            logging.error("Failed to connect to MongoDB.")

    def get_synotoken(branch, api_url, account, passwd):
        """Function to fetch synotoken for an API"""
        logging.info(f'Requesting synotoken for API {branch}...')
        url = api_url + '/webapi/entry.cgi'  # Fetching API URL from environment variables
        raw_data = f'api=SYNO.API.Auth&version=7&method=login&enable_syno_token=yes&account={account}&passwd={passwd}&rememberme=1'
        headers = {'cookie': 'type=tunnel;'}

        try:
            response = requests.post(url, data=raw_data, headers=headers)
            response.raise_for_status()  # Raise an exception for non-2xx status codes
            if response.status_code == 200:
                logging.info('Synotoken request successful!')
                cookies = response.cookies
                did = cookies.get('did')
                id_ = cookies.get('id')

                if did and id_:
                    cookies_str = f'did={did}; id={id_}'
                    json_data = response.json()

                    if 'data' in json_data:
                        synotoken = json_data['data'].get('synotoken', '')

                        credentials_cache[branch] = {
                            'synotoken': synotoken,
                            'cookies_str': cookies_str
                        }

                        save_cache(credentials_cache, cache_file)
                else:
                    logging.error("Cookie 'did' or 'id' not found.")
            else:
                logging.error(f'Request failed with status code: {response.status_code}')
        except requests.RequestException as e:
            logging.error(f"Error requesting synotoken: {e}")

    def get_recordings(branch, api_url, timeout, from_time, to_time):
        """Function to get recordings for an API"""
        logging.info(f'Getting Recording list for branch: {branch}')
        credentials = credentials_cache.get(branch)
        if not credentials:
            logging.info(f'Credentials not found for branch: {branch}. Fetching synotoken...')
            # If credentials are not cached, fetch synotoken
            account = 'dx-auto'
            passwd = 'Hana@123!'
            get_synotoken(branch, api_url, account, passwd)
            credentials = credentials_cache.get(branch)

        if credentials:
            synotoken = credentials['synotoken']
            cookies_str = credentials['cookies_str']
            url = api_url + '/webapi/entry.cgi'  # Constructing complete URL
            raw_data = f'start=0&limit=0&offsetIdMap=%22%7B%7D%22&timestampMap=%22%7B%7D%22&cameraIds=%22%22&triggerLabel=%5B0%5D&fromTime={from_time}&toTime={to_time}&dayRangeFrom=0&dayRangeTo=0&frequency=%22%22&locked=0&evtSrcType=2&evtSrcId=0&blIncludeSnapshot=true&timezoneOffset=390&blonline_ds_only=true&applyString=%220%22&bookmarkKeyword=%22%22&includeAllCam=true&labelOper=0&systemLabel=0&customLabel=0&comment=%22%22&from_start=0&from_end=0&clientDayRangeFrom=%2200%3A00%22&clientDayRangeTo=%2200%3A15%22&chkRecMode=false&edgeFilterType=0&blTotalCntOnly=true&api=SYNO.SurveillanceStation.Recording&method=List&version=5'
            headers = {
                'x-syno-token': synotoken,
                'cookie': f'type=tunnel;{cookies_str}'
            }

            try:
                response = requests.post(url, data=raw_data, headers=headers, timeout=TIMEOUT)
                response.raise_for_status()  # Raise an exception for non-2xx status codes
                if response.status_code == 200:
                    logging.info('Getting recording lists successful!')
                    json_data = response.json()
                    if 'data' in json_data:
                        recordings_data = json_data['data'].get('events', [])
                        if isinstance(recordings_data, list):
                            return recordings_data
                        else:
                            logging.error('The JSON response is not an array.')
            except requests.exceptions.Timeout:
                logging.error("Request timed out after {} seconds".format(timeout))
            except requests.RequestException as e:
                logging.error(f"Request error: {e}")

        return None

    all_recordings_data = []

    cache_file = 'cache.json'

    credentials_cache = load_cache(cache_file)

    # Get the API URLs from environment variables
    json_string = os.environ.get('REGION_DATA')
    if json_string:
        # Convert the JSON string to a dictionary
        region_data = json.loads(json_string)
        logging.info("Region data loaded.")
    else:
        logging.error("REGION_DATA environment variable not found or is empty.")
        region_data = {}

    # If region_data is empty, log a message and exit the function
    if not region_data:
        logging.warning("No region data found.")
        return {
            "statusCode": 500,
            "body": "No region data found."
        }

    # Calculate timeout based on the number of API endpoints
    TIMEOUT_MINUTES = 10
    TIMEOUT = (TIMEOUT_MINUTES * 60) / len(region_data)

    end_date = datetime.now()
    # Calculate the start time as 45 minutes before the current time
    start_date = end_date - timedelta(minutes=45)

    from_time = int(start_date.timestamp())
    to_time = int(end_date.timestamp())

    logging.info(f'Timeout: {TIMEOUT}s')
    logging.info(f'From Time: {start_date}, To Time: {end_date}')

    if region_data:
        for branch, url in region_data.items():
            recordings = get_recordings(branch, url, TIMEOUT, from_time, to_time)
            if recordings:
                branch_recordings_data = []
                for recording in recordings:
                    start_time = pd.to_datetime(recording['startTime'], unit='s') + pd.Timedelta(hours=6.5)
                    stop_time = pd.to_datetime(recording['stopTime'], unit='s') + pd.Timedelta(hours=6.5)
                    selected_recording = {
                        'eventId': recording['eventId'],
                        'region': get_region(branch),
                        'br-code': branch,
                        'camera_name': recording['camera_name'],
                        'startTime': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'stopTime': stop_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'duration(minutes)': math.floor((stop_time - start_time) / pd.Timedelta(minutes=1)),  # Calculate duration in minutes and round down
                        'eventSize(MB)': recording['eventSize'],
                        'recording': recording['status'],
                    }
                    branch_recordings_data.append(selected_recording)
                logging.info(f"Recordings for branch {branch} processed.")
                # Insert recordings for this branch into MongoDB
                insert_to_mongodb(branch_recordings_data, 'recordings')
            else:
                logging.warning(f"No recordings found for API {branch}.")
    else:
        logging.warning("No region data found.")

    # Save cache to JSON file
    save_cache(credentials_cache, cache_file)
    logging.info('Program ends')
    return {
        "statusCode": 200,
        "body": 'succeed'
    }


lambda_handler(None, None)
