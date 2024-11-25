import csv
import json
import os
from datetime import datetime
from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from flask_cors import CORS
from bson.objectid import ObjectId
from dotenv import load_dotenv
import io
import pandas as pd
import xmltodict
import requests
import traceback

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
    sales = sales_collection.find()
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

# 4. Upload and Preview Endpoint
@app.route('/api/upload-preview', methods=['POST'])
def upload_preview():
    if 'file' not in request.files:
        return jsonify({'message': 'No file part in the request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'message': 'No file selected for uploading'}), 400
    try:
        filename = file.filename.lower()
        if filename.endswith('.json'):
            new_data = process_json(file)
        elif filename.endswith('.csv'):
            new_data = process_csv(file)
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            new_data = process_excel(file)
        elif filename.endswith('.xml'):
            new_data = process_xml(file)
        else:
            return jsonify({'message': 'Invalid file type. Please upload a JSON, CSV, XLSX, XLS, or XML file.'}), 400
        # Return the new data for preview without merging
        return jsonify({'message': 'File uploaded for preview', 'new_data': new_data}), 200
    except Exception as e:
        return jsonify({'message': f'Error processing file: {str(e)}'}), 500

# 5. Get Existing Data Schema
@app.route('/api/sales-schema', methods=['GET'])
def get_sales_schema():
    sales = sales_collection.find_one()
    if sales:
        schema = get_schema(sales)
        return jsonify({'schema': schema}), 200
    else:
        return jsonify({'schema': []}), 200  # No data in the collection

def get_schema(document):
    return list(document.keys())

# 6. Merge Data Endpoint
@app.route('/api/merge-data', methods=['POST'])
def merge_data():
    data = request.json
    new_data = data.get('new_data')
    selected_schema = data.get('selected_schema')  # 'original' or 'new'

    if not new_data or not selected_schema:
        return jsonify({'message': 'Missing new data or selected schema'}), 400

    # Adjust new_data based on selected schema
    if selected_schema == 'original':
        # Get original schema
        sales = sales_collection.find_one()
        if sales:
            original_schema = set(sales.keys())
            for item in new_data:
                # Remove keys not in original schema
                keys_to_remove = set(item.keys()) - original_schema
                for key in keys_to_remove:
                    del item[key]
        else:
            # No existing data, use new data as is
            pass
    elif selected_schema == 'new':
        # Optionally, update existing data to match new schema
        pass  # For simplicity, we won't modify existing data here

    # Trigger backup before merging
    backup_name = backup_sales_collection()

    # Insert the adjusted new data
    sales_collection.insert_many(new_data)

    return jsonify({'message': 'Data merged successfully', 'backup_name': backup_name}), 200

def process_json(file):
    """ Process uploaded JSON file """
    data = json.load(file.stream)
    # Ensure numerical fields are correct types
    for record in data:
        record['sales_amount'] = float(record.get('sales_amount', 0))
        record['units_sold'] = int(record.get('units_sold', 0))
    return data

def process_csv(file):
    """ Process uploaded CSV file """
    try:
        # Read the file content and decode it
        file_contents = file.read().decode('utf-8', errors='ignore')
        decoded_file = io.StringIO(file_contents)
        csv_reader = csv.DictReader(decoded_file)
        data = []
        for row in csv_reader:
            # Convert numerical fields safely
            try:
                row['sales_amount'] = float(row.get('sales_amount', 0))
            except ValueError:
                row['sales_amount'] = 0.0  # Default value or handle as needed
            try:
                row['units_sold'] = int(row.get('units_sold', 0))
            except ValueError:
                row['units_sold'] = 0  # Default value or handle as needed
            # Nest 'location' and 'gender' under 'customer'
            customer_info = {
                'location': row.pop('location', None),
                'gender': row.pop('gender', None)
            }
            row['customer'] = customer_info
            data.append(row)
        return data
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error processing CSV file: {str(e)}")

def process_excel(file):
    """ Process uploaded Excel file """
    try:
        df = pd.read_excel(file)
        # Ensure numerical fields are correct types
        df['sales_amount'] = df['sales_amount'].astype(float)
        df['units_sold'] = df['units_sold'].astype(int)
        data = df.to_dict(orient='records')
        return data
    except Exception as e:
        raise Exception(f"Error reading Excel file: {str(e)}")

def process_xml(file):
    """ Process uploaded XML file """
    try:
        content = file.read()
        xml_dict = xmltodict.parse(content)

        # Function to recursively find the list of records
        def find_records(node):
            if isinstance(node, list):
                return node
            elif isinstance(node, dict):
                for key, value in node.items():
                    records = find_records(value)
                    if records is not None:
                        return records
            return None

        records = find_records(xml_dict)
        if records is None:
            raise Exception("Could not find records in XML file")

        data = []
        for record in records:
            # Convert OrderedDict to dict
            record = dict(record)
            # Convert numerical fields to appropriate types
            record['sales_amount'] = float(record.get('sales_amount', 0))
            record['units_sold'] = int(record.get('units_sold', 0))
            # Process nested 'customer' data
            if 'customer' in record:
                record['customer'] = dict(record['customer'])
            data.append(record)
        return data
    except Exception as e:
        raise Exception(f"Error processing XML file: {str(e)}")

# 7. Restore Backup Endpoint
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

# 8. Upload and Merge Data Endpoint
@app.route('/api/upload', methods=['POST'])
def upload_and_merge_data():
    if 'file' not in request.files:
        return jsonify({'message': 'No file part in the request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'message': 'No file selected for uploading'}), 400
    try:
        filename = file.filename.lower()
        if filename.endswith('.json'):
            new_data = process_json(file)
        elif filename.endswith('.csv'):
            new_data = process_csv(file)
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            new_data = process_excel(file)
        elif filename.endswith('.xml'):
            new_data = process_xml(file)
        else:
            return jsonify({'message': 'Invalid file type. Please upload a JSON, CSV, XLSX, XLS, or XML file.'}), 400

        # Get existing sales data from the database
        local_data_cursor = sales_collection.find()
        local_data = list(local_data_cursor)
        # Convert ObjectId to string in local_data
        for doc in local_data:
            doc['_id'] = str(doc['_id'])

        # Prepare payload for the merge request
        payload = {
            'local_data': local_data,
            'new_data': new_data
        }

        # Retrieve the API token
        api_token = os.environ.get('API_TOKEN')

        # Set the headers with the Authorization token
        headers = {
            'Authorization': f'Bearer {api_token}'
        }

        # Send the merge request to the other server
        merge_url = 'http://167.172.135.70:5000/merge'

        response = requests.post(merge_url, json=payload, headers=headers)

        if response.status_code == 200:
            # Parse the merged data from the response
            merged_data = response.json().get('merged_data')

            if merged_data:
                # Backup existing sales data
                backup_name = backup_sales_collection()

                # Replace existing data in the database with the merged data
                sales_collection.delete_many({})

                # Convert '_id' fields back to ObjectId
                for doc in merged_data:
                    if '_id' in doc:
                        doc['_id'] = ObjectId(doc['_id'])

                sales_collection.insert_many(merged_data)

                return jsonify({'message': 'Data merged successfully', 'backup_name': backup_name}), 200
            else:
                return jsonify({'message': 'Merge failed: No merged data received from the server'}), 500
        else:
            return jsonify({'message': f'Merge failed: {response.text}'}), response.status_code
    except Exception as e:
        traceback.print_exc()
        return jsonify({'message': f'Error processing file: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)