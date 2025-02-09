# CCTV Recording Data Retrieval and Analysis

### Overview

This project retrieves CCTV recording data from Hana Microfinance Company's branches, analyzes the data, and pushes it to a MongoDB server. The data is fetched from the `hana.quickconnectto` API using the `webapi/entry.cgi` endpoint. The project utilizes Docker to set up the environment, AWS ECR for container image storage, and AWS Lambda for scheduling the data retrieval process.

### Getting Started

To run this project locally or deploy it to AWS Lambda, follow these steps:

- Clone the repository:

   ```bash
   git clone https://github.com/EiPhyuSinn/CCTV-Recording-Data-Retrieval-and-Analysis
    ```
- Install the required dependencies
  

### Set up the environment variables:Create a .env file in the root directory and add the following variables:

- Replace placeholders with actual values.


### Architecture
The project architecture consists of the following components:

- Data Retrieval: CCTV recording data is fetched using the webapi/entry.cgi endpoint.
- Data Analysis: The fetched data is analyzed to extract important information.
- MongoDB: Data is stored in a MongoDB database hosted on an AWS EC2 instance.
- Docker: The project can be containerized using Docker for easy deployment.
- AWS Lambda: Scheduled Lambda functions trigger the data retrieval process every 5 minutes.
- Environment Variables
- MONGODB_URL_STRING: Connection string for MongoDB.
- REGION_DATA: Dictionary mapping region names to API URLs for CCTV data retrieval.


