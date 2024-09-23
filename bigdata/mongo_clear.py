from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["vehicle_db"]

# Delete all documents from the processed_vehicle_positions collection
db["processed_vehicle_positions"].delete_many({})

# Delete all documents from the raw_vehicle_positions collection
db["raw_vehicle_positions"].delete_many({})

print("All documents deleted from 'processed_vehicle_positions' and 'raw_vehicle_positions' collections.")
