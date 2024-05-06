from datetime import datetime, timedelta
import os
import mysql.connector
from pymongo import MongoClient, UpdateOne

DB_HOSTNAME = os.environ.get('DB_HOSTNAME')
DB_DATABASE = os.environ.get('DB_DATABASE')
DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

MONGODB_URL_STRING = os.environ.get('MONGODB_URL_STRING')

REPORT_TYPES = os.environ.get('REPORT_TYPES')

def mongoconnect():
  try:
    client = MongoClient(MONGODB_URL_STRING)
  except:
    print("Can't connect to mongo database")
    return 0

  print("Mongo Connected")
  return client


def mysqlconnect():
  try:
    connection = mysql.connector.connect(
      host=DB_HOSTNAME,
      user=DB_USERNAME,
      password=DB_PASSWORD,
      database=DB_DATABASE
    )
  # If connection is not successful
  except:
    print("Can't connect to mysql database")
    return 0
  # If Connection Is Successful
  print("MySQL Connected")
  return connection


def getResult(connection, sql):
  cursor = connection.cursor()

  try: 
    cursor.execute(sql)
    return cursor.fetchall()
  except:
    print("Somethings went wrong!")
    return []


def generateClients(e, mysqlConn, mongoDb):
  clients = mongoDb.clients
  lookup = mongoDb.lookup

  if 'start_date' in e and 'end_date' in e:
    startDate = e['start_date']
    endDate = e['end_date']
    lastUpdatedAt = datetime.strptime(endDate, "%Y-%m-%d")
    sql = f"SELECT id, is_lock, activated_from, created_at, updated_at FROM db_clients WHERE created_at BETWEEN '{startDate}' and '{endDate}' and deleted_at IS NULL ORDER BY created_at;"
  else :
    status = lookup.find_one({'type': 'clients'})
    lookup.update_one({"_id": status['_id'],}, {"$set": {"status": "pending"}})
    startDate = status['lastUpdatedAt'] + timedelta(seconds=1)
    endDate = startDate + timedelta(hours=1)
    lastUpdatedAt = endDate
    sql = f"SELECT id, is_lock, activated_from, created_at, updated_at FROM db_clients WHERE updated_at BETWEEN '{startDate}' and '{endDate}' and deleted_at IS NULL ORDER BY updated_at;"
  
  print(f"Select clients from {startDate} to {endDate}.")

  result = getResult(mysqlConn, sql)

  bulkOperations = []
  for row in result:
    query = { '_id': row[0] }
    document = {
      "$set": {
        "_id": row[0],
        "isLock": row[1],
        "activatedFrom": row[2],
        "createdAt": row[3],
        "updatedAt": row[4],
      },
      "$setOnInsert": {"insertedAt": datetime.now()}
    }
    bulkOperations.append(UpdateOne(query, document, upsert=True))

  if bulkOperations:
    clients.bulk_write(bulkOperations)

  lookup.update_one({"type": "clients",}, 
    {
      "$set": {
        "lastUpdatedAt": lastUpdatedAt,
        "status": "success"
      }
    }
  )

  print(f"Generate clients({len(bulkOperations)}) successful.")


def generateInterviews(e, mysqlConn, mongoDb):
  interviews = mongoDb.interviews
  lookup = mongoDb.lookup

  if 'start_date' in e and 'end_date' in e:
    startDate = e['start_date']
    endDate = e['end_date']
    lastUpdatedAt = datetime.strptime(endDate, "%Y-%m-%d")
    sql = f"SELECT id, interview_status, loan_type, sync_status, created_at, updated_at FROM db_interviews WHERE deleted_at IS NULL AND created_at BETWEEN '{startDate}' and '{endDate}' ORDER BY created_at;"
  else :
    status = lookup.find_one({'type': 'interviews'})
    lookup.update_one({"_id": status['_id']}, {"$set": {"status": "pending"}})
    startDate = status['lastUpdatedAt'] + timedelta(seconds=1)
    endDate = startDate + timedelta(hours=1)
    lastUpdatedAt = endDate
    sql = f"SELECT id, interview_status, loan_type, sync_status, created_at, updated_at FROM db_interviews WHERE deleted_at IS NULL AND updated_at BETWEEN '{startDate}' and '{endDate}' ORDER BY updated_at;"
  
  print(f"Select interviews from {startDate} to {endDate}.")

  result = getResult(mysqlConn, sql)

  bulkOperations = []
  for row in result:
    query = { '_id': row[0] }
    document = {
      "$set": {
        "_id": row[0],
        "interviewStatus": row[1],
        "loanType": row[2],
        "syncStatus": row[3],
        "createdAt": row[4],
        "updatedAt": row[5],
      },
      "$setOnInsert": {"insertedAt": datetime.now()}
    }
    bulkOperations.append(UpdateOne(query, document, upsert=True))

  if bulkOperations:
    interviews.bulk_write(bulkOperations)

  lookup.update_one({'type': 'interviews'}, 
    {
      "$set": {
        "lastUpdatedAt": lastUpdatedAt,
        "status": "success"
      }
    }
  )

  print(f"Generate interviews({len(bulkOperations)}) successful.")


def generateDisbursements(e, mysqlConn, mongoDb):
  disbursalLoans = mongoDb.disbursalLoans
  lookup = mongoDb.lookup

  if 'start_date' in e and 'end_date' in e:
    startDate = e['start_date']
    endDate = e['end_date']
    lastUpdatedAt = datetime.strptime(endDate, "%Y-%m-%d")
    sql = f"SELECT id, fo_action, disbursement_status, app_type, created_at, updated_at FROM db_disbursal_loans WHERE fo_action IS NOT NULL AND disbursement_status IS NOT NULL AND sync_status = 'success' AND deleted_at IS NULL AND created_at BETWEEN '{startDate}' and '{endDate}' ORDER BY created_at;"
  else :
    status = lookup.find_one({'type': 'disbursalLoans'})
    lookup.update_one({"_id": status['_id']}, {"$set": {"status": "pending"}})
    startDate = status['lastUpdatedAt'] + timedelta(seconds=1)
    endDate = startDate + timedelta(hours=1)
    lastUpdatedAt = endDate
    sql = f"SELECT id, fo_action, disbursement_status, app_type, created_at, updated_at FROM db_disbursal_loans WHERE fo_action IS NOT NULL AND disbursement_status IS NOT NULL AND sync_status = 'success' AND deleted_at IS NULL AND updated_at BETWEEN '{startDate}' and '{endDate}' ORDER BY updated_at;"

  print(f"Select disbursal loans from {startDate} to {endDate}.")

  result = getResult(mysqlConn, sql)

  bulkOperations = []
  for row in result:
    query = { '_id': row[0] }
    document = {
      "$set": {
        "_id": row[0],
        "foAction": row[1],
        "disbursementStatus": row[2],
        "appType": row[3],
        "createdAt": row[4],
        "updatedAt": row[5],
      },
      "$setOnInsert": {"insertedAt": datetime.now()}
    }
    bulkOperations.append(UpdateOne(query, document, upsert=True))

  if bulkOperations:
    disbursalLoans.bulk_write(bulkOperations)
  
  lookup.update_one({'type': 'disbursalLoans'}, 
    {
      "$set": {
        "lastUpdatedAt": lastUpdatedAt,
        "status": "success"
      }
    }
  )

  print(f"Generate disbursal loans({len(bulkOperations)}) successful.")


def generateLoanCollections(e, mysqlConn, mongoDb):
  loanCollections = mongoDb.loanCollections
  lookup = mongoDb.lookup

  if 'start_date' in e and 'end_date' in e:
    startDate = e['start_date']
    endDate = e['end_date']
    lastUpdatedAt = datetime.strptime(endDate, "%Y-%m-%d")
    sql = f"SELECT id, app_type, type, created_at, updated_at FROM db_loan_repayment_transactions WHERE sync_status='success' AND status='repayment' AND deleted_at IS NULL AND created_at BETWEEN '{startDate}' and '{endDate}' ORDER BY created_at;"
  else :
    status = lookup.find_one({'type': 'loanCollections'})
    lookup.update_one({"_id": status['_id']}, {"$set": {"status": "pending"}})
    startDate = status['lastUpdatedAt'] + timedelta(seconds=1)
    endDate = startDate + timedelta(hours=1)
    lastUpdatedAt = endDate
    sql = f"SELECT id, app_type, type, created_at, updated_at FROM db_loan_repayment_transactions WHERE sync_status='success' AND status='repayment' AND deleted_at IS NULL AND updated_at BETWEEN '{startDate}' and '{endDate}' ORDER BY updated_at;"

  print(f"Select loan collections from {startDate} to {endDate}.")
  
  result = getResult(mysqlConn, sql)

  bulkOperations = []
  for row in result:
    query = { "_id": row[0] }
    document = {
      "$set": {
        "_id": row[0],
        "appType": row[1],
        "type": row[2],
        "createdAt": row[3],
        "updatedAt": row[4],
      },
      "$setOnInsert": {"insertedAt": datetime.now()}
    }
    bulkOperations.append(UpdateOne(query, document, upsert=True))

  if bulkOperations:
    loanCollections.bulk_write(bulkOperations)
  
  lookup.update_one({'type': 'loanCollections'}, 
    {
      "$set": {
        "lastUpdatedAt": lastUpdatedAt,
        "status": "success"
      }
    }
  )

  print(f"Generate loan collections({len(bulkOperations)}) successful.")


def generateSaving(e, mysqlConn, mongoDb):
  savings = mongoDb.savings
  lookup = mongoDb.lookup

  if 'start_date' in e and 'end_date' in e:
    startDate = e['start_date']
    endDate = e['end_date']
    lastUpdatedAt = datetime.strptime(endDate, "%Y-%m-%d")
    sql = f"SELECT id, type, app_type, status, created_at, updated_at FROM db_saving_transactions WHERE sync_status = 'success' AND saving_product_type = 'vsaving' AND status IN ('deposit', 'withdraw') AND deleted_at IS NULL AND created_at BETWEEN '{startDate}' and '{endDate}' ORDER BY created_at;"
  else :
    status = lookup.find_one({'type': 'savings'})
    lookup.update_one({"_id": status['_id']}, {"$set": {"status": "pending"}})
    startDate = status['lastUpdatedAt'] + timedelta(seconds=1)
    endDate = startDate + timedelta(hours=1)
    lastUpdatedAt = endDate
    sql = f"SELECT id, type, app_type, status, created_at, updated_at FROM db_saving_transactions WHERE sync_status = 'success' AND saving_product_type = 'vsaving' AND status IN ('deposit', 'withdraw') AND deleted_at IS NULL AND updated_at BETWEEN '{startDate}' and '{endDate}' ORDER BY updated_at;"

  print(f"Select savings from {startDate} to {endDate}.")

  result = getResult(mysqlConn, sql)

  bulkOperations = []
  for row in result:
    query = { '_id': row[0] }
    document = {
      "$set": {
        "_id": row[0],
        "type": row[1],
        "appType": row[2],
        "status": row[3],
        "createdAt": row[4],
        "updatedAt": row[5],
      },
      "$setOnInsert": {"insertedAt": datetime.now()}
    }
    bulkOperations.append(UpdateOne(query, document, upsert=True))

  if bulkOperations:
    savings.bulk_write(bulkOperations)
  
  lookup.update_one({'type': 'savings'}, 
    {
      "$set": {
        "lastUpdatedAt": lastUpdatedAt,
        "status": "success"
      }
    }
  )

  print(f"Generate savings({len(bulkOperations)}) successful.")


def handler(event, context):
  connection = mysqlconnect()
  client = mongoconnect()
  db = client.reports

  print(client.list_database_names())

  reportTypes = REPORT_TYPES.split(',')
  if "clients" in reportTypes:
    generateClients(event, connection, db)

  if "interviews" in reportTypes:
    generateInterviews(event, connection, db)

  if "disbursalLoans" in reportTypes:
    generateDisbursements(event, connection, db)

  if "loanCollections" in reportTypes:
    generateLoanCollections(event, connection, db)

  if "savings" in reportTypes:
    generateSaving(event, connection, db)

  # Closing Database Connection
  connection.close()
  client.close()

  return {
    "statusCode": 200,
    "body": "Successful"
  }