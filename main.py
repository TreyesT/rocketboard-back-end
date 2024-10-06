import csv

from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from flask_cors import CORS
import json
import os

app = Flask(__name__)
CORS(app)
# MongoDB configuration (replace with your MongoDB URI)
app.config["MONGO_URI"] = os.environ.get("MONGO_URI", "mongodb://mongo:27017/mydb")
mongo = PyMongo(app)


# Access collections
sales_collection = mongo.db.sales
analytics_collection = mongo.db.analytics
tasks_collection = mongo.db.tasks

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "OK"}), 200


# 1. Insert Sales Data
@app.route('/api/sales', methods=['POST'])
def insert_sales_data():
    data = request.json
    sales_id = sales_collection.insert_one(data).inserted_id
    return jsonify({"message": "Sales data inserted", "id": str(sales_id)}), 201

# 2. Insert Web Analytics Data
@app.route('/api/analytics', methods=['POST'])
def insert_analytics_data():
    data = request.json
    analytics_id = analytics_collection.insert_one(data).inserted_id
    return jsonify({"message": "Analytics data inserted", "id": str(analytics_id)}), 201

# 3. Insert Task Data for Project Management
@app.route('/api/tasks', methods=['POST'])
def insert_task_data():
    data = request.json
    task_id = tasks_collection.insert_one(data).inserted_id
    return jsonify({"message": "Task data inserted", "id": str(task_id)}), 201


@app.route('/api/sales', methods=['GET'])
def get_sales_data():
    sales = mongo.db.sales.find()  # Replace 'sales' with your collection name
    sales_list = []
    for sale in sales:
        sale['_id'] = str(sale['_id'])  # Convert ObjectId to string
        sales_list.append(sale)
    return jsonify(sales_list), 200


@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'message': 'No file part in the request'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'message': 'No file selected for uploading'}), 400

    if file and file.filename.endswith('.json'):
        sales_data = process_json(file)

        # Remove _id field to avoid duplicate key errors
        for sale in sales_data:
            sale.pop('_id', None)  # Remove the _id if it exists

        # Insert or merge the uploaded data
        sales_collection = mongo.db.sales
        sales_collection.insert_many(sales_data)  # Adjust to insert many

    return jsonify({'message': 'File uploaded and data merged successfully'}), 201







def process_csv(file):
    """ Process uploaded CSV file """
    sales_data = []
    csv_file = csv.DictReader(file.stream)  # Read CSV file as a stream
    for row in csv_file:
        sales_data.append(row)  # Add rows to sales_data list
    return sales_data

def process_json(file):
    """ Process uploaded JSON file """
    data = json.load(file.stream)  # Read and load the JSON file
    return data


# # Backup function
# def backup_sales_collection():
#     backup_collection_name = f"sales_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
#     backup_collection = mongo.db[backup_collection_name]
#
#     # Copy data from the sales collection to the backup collection
#     sales_data = list(sales_collection.find({}))
#     if sales_data:
#         backup_collection.insert_many(sales_data)
#     return backup_collection_name  # Return the backup collection name

#
# # Restore function (reverts to backup)
# def restore_sales_collection(backup_collection_name):
#     backup_collection = mongo.db[backup_collection_name]
#
#     # Clear the sales collection and restore from the backup
#     sales_collection.delete_many({})  # Clear the current sales data
#     backup_data = list(backup_collection.find({}))
#     if backup_data:
#         sales_collection.insert_many(backup_data)
#
#
# # Endpoint to trigger a backup before a merge
# @app.route('/backup-and-merge', methods=['POST'])
# def backup_and_merge():
#     # Backup current data before merging
#     backup_name = backup_sales_collection()
#
#     # Proceed with merging new data
#     new_data = request.json.get('new_data', [])
#     if new_data:
#         for sale in new_data:
#             sales_collection.update_one(
#                 {'_id': sale['_id']},  # Match by _id
#                 {'$set': sale},  # Update the document
#                 upsert=True  # Insert if it doesn't exist
#             )
#
#     return jsonify({
#         'message': 'Data merged successfully',
#         'backup_name': backup_name  # Return the backup name
#     }), 200
#
#
# # Endpoint to revert (restore) the data from a backup
# @app.route('/restore-backup', methods=['POST'])
# def restore_backup():
#     backup_collection_name = request.json.get('backup_name')
#     if not backup_collection_name:
#         return jsonify({'error': 'No backup name provided'}), 400
#
#     try:
#         restore_sales_collection(backup_collection_name)
#         return jsonify({'message': f'Restored from backup: {backup_collection_name}'}), 200
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 5000)