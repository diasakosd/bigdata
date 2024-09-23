from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["vehicle_db"]

def get_vehicle_count_least_edge(start_time, end_time):
    collection = db["processed_vehicle_positions"]
    print("Querying vehicle count for least edge...")

    # Find the minimum number of vehicles
    min_vehicles_result = collection.aggregate([
        {"$match": {"window.start": {"$gte": start_time}, "window.end": {"$lte": end_time}}},
        {"$group": {"_id": "$link", "total_vehicles": {"$sum": "$vcount"}}},
        {"$sort": {"total_vehicles": 1}},
        {"$limit": 1}
    ])
    
    min_vehicles_list = list(min_vehicles_result)
    if min_vehicles_list:
        min_vehicles = min_vehicles_list[0]['total_vehicles']
        # Find all links with the minimum number of vehicles
        result = collection.aggregate([
            {"$match": {"window.start": {"$gte": start_time}, "window.end": {"$lte": end_time}}},
            {"$group": {"_id": "$link", "total_vehicles": {"$sum": "$vcount"}}},
            {"$match": {"total_vehicles": min_vehicles}}
        ])
        result_list = list(result)
        if result_list:
            for r in result_list:
                print(f"Link with the least number of vehicles: {r['_id']} with count {r['total_vehicles']}")
        else:
            print("No results found for vehicle count query.")
    else:
        print("No results found for vehicle count query.")

def get_highest_avg_speed_edge(start_time, end_time):
    collection = db["processed_vehicle_positions"]
    print("Querying highest average speed for edge...")

    # Find the maximum average speed
    max_speed_result = collection.aggregate([
        {"$match": {"window.start": {"$gte": start_time}, "window.end": {"$lte": end_time}}},
        {"$group": {"_id": "$link", "average_speed": {"$avg": "$vspeed"}}},
        {"$sort": {"average_speed": -1}},
        {"$limit": 1}
    ])
    
    max_speed_list = list(max_speed_result)
    if max_speed_list:
        max_speed = max_speed_list[0]['average_speed']
        # Find all links with the maximum average speed
        result = collection.aggregate([
            {"$match": {"window.start": {"$gte": start_time}, "window.end": {"$lte": end_time}}},
            {"$group": {"_id": "$link", "average_speed": {"$avg": "$vspeed"}}},
            {"$match": {"average_speed": max_speed}}
        ])
        result_list = list(result)
        if result_list:
            for r in result_list:
                print(f"Link with the highest average speed: {r['_id']} with average speed {r['average_speed']}")
        else:
            print("No results found for average speed query.")
    else:
        print("No results found for average speed query.")

def get_longest_distance(start_time, end_time):
    collection = db["processed_vehicle_positions"]
    print("Querying longest distance...")

    # Use the pipeline to calculate the distance and retrieve additional fields
    distance_pipeline = [
        {"$match": {
            "window.start": {"$gte": start_time}, 
            "window.end": {"$lte": end_time},
            "vspeed": {"$gt": 0},  # Ensure speed is greater than zero
            "duration_since_start": {"$gt": 0}  # Ensure duration is greater than zero
        }},
        {"$project": {
            "link": 1,
            "window": 1,
            "vspeed": 1,
            "duration_since_start": 1,
            "vcount": 1,
            "distance": {"$multiply": ["$duration_since_start", "$vspeed"]}  # Calculate distance as duration * speed
        }},
        {"$sort": {"distance": -1}},  # Sort by distance in descending order
        {"$limit": 1}  # Get the entry with the longest distance
    ]

    result = list(collection.aggregate(distance_pipeline))
    if result:
        for r in result:
            link = r['link']
            total_distance = r['distance']
            window = r['window']
            vspeed = r['vspeed']
            duration = r['duration_since_start']
            vcount = r['vcount']
            
            print(f"Link: {link}")
            print(f"Window: {window}")
            print(f"Average Speed (vspeed): {vspeed} km/h")
            print(f"Duration Since Start: {duration} seconds")
            print(f"Vehicle Count (vcount): {vcount}")
            print(f"Longest Total Distance: {total_distance} meters")
    else:
        print("No results found for longest distance query.")





# Define time period for queries
start_time = datetime(2024, 9, 2, 12, 28, 0)  # Adjusted to match ISODate format in MongoDB
end_time = datetime.now()  # Set end time to the current time

# Execute queries
get_vehicle_count_least_edge(start_time, end_time)
get_highest_avg_speed_edge(start_time, end_time)
get_longest_distance(start_time, end_time)
