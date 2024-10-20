import csv
import json
import os
from datetime import datetime
from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from flask_cors import CORS
from bson.objectid import ObjectId
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

app = Flask(__name__)
CORS(app)

app.config["MONGO_URI"] = os.environ.get("MONGO_URI")
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

# 2. Get Sales Data
@app.route('/api/sales', methods=['GET'])
def get_sales_data():
    sales = mongo.db.sales.find()
    sales_list = []
    for sale in sales:
        sale['_id'] = str(sale['_id'])  # Convert ObjectId to string
        sales_list.append(sale)
    return jsonify(sales_list), 200

# 3. List Backups Endpoint
@app.route('/api/list-backups', methods=['GET'])
def list_backups():
    collections = mongo.db.list_collection_names()
    backup_collections = [col for col in collections if col.startswith('sales_backup')]
    return jsonify({'backups': backup_collections}), 200

# Backup function
def backup_sales_collection():
    backup_collection_name = f"sales_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_collection = mongo.db[backup_collection_name]
    sales_data = list(sales_collection.find({}))
    if sales_data:
        backup_collection.insert_many(sales_data)
    return backup_collection_name

# Restore function
def restore_sales_collection(backup_collection_name):
    backup_collection = mongo.db[backup_collection_name]
    sales_collection.delete_many({})  # Clear the current sales data
    backup_data = list(backup_collection.find({}))
    if backup_data:
        sales_collection.insert_many(backup_data)

# 4. Upload and Merge Endpoint (with backup before merge)
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
            sale.pop('_id', None)

        # Trigger backup before merging
        backup_name = backup_sales_collection()

        # Insert the uploaded data
        sales_collection.insert_many(sales_data)

    return jsonify({'message': 'File uploaded and data merged successfully', 'backup_name': backup_name}), 201

def process_json(file):
    """ Process uploaded JSON file """
    data = json.load(file.stream)
    return data

# 5. Restore Backup Endpoint
@app.route('/api/restore-backup', methods=['POST'])
def restore_backup():
    backup_collection_name = request.json.get('backup_name')
    if not backup_collection_name:
        return jsonify({'error': 'No backup name provided'}), 400
    try:
        restore_sales_collection(backup_collection_name)
        return jsonify({'message': f'Restored from backup: {backup_collection_name}'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500





if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 5000)