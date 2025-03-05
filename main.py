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

# # 5. Get Existing Data Schema
# @app.route('/api/sales-schema', methods=['GET'])
# def get_sales_schema():
#     sales = sales_collection.find_one()
#     if sales:
#         schema = get_schema(sales)
#         return jsonify({'schema': schema}), 200
#     else:
#         return jsonify({'schema': []}), 200  # No data in the collection

@app.route('/api/process-merge-mappings', methods=['POST'])
def process_merge_with_mappings():
    try:
        data = request.json
        new_data = data.get('new_data', [])
        field_mappings = data.get('field_mappings', {}).get('mappings', [])
        matching_fields = data.get('matching_fields', [])

        if not new_data or not field_mappings:
            return jsonify({
                'message': 'Missing new data or field mappings',
                'status': 'error'
            }), 400

        # Create a dictionary for easier access to mappings
        mapping_dict = {mapping['existing']: mapping['new'] for mapping in field_mappings if mapping['new']}

        # Transform the new data according to the mappings
        transformed_data = []
        for record in new_data:
            transformed_record = {}

            # Apply field mappings, creating nested objects as needed
            for existing_field, new_field in mapping_dict.items():
                if new_field in record:
                    # Handle nested fields in existing_field
                    if '.' in existing_field:
                        parts = existing_field.split('.')
                        current = transformed_record

                        # Build the nested structure
                        for i, part in enumerate(parts):
                            if i == len(parts) - 1:
                                # Last part is the actual field
                                current[part] = record[new_field]
                            else:
                                # Create nested objects if they don't exist
                                if part not in current:
                                    current[part] = {}
                                current = current[part]
                    else:
                        # Simple field, no nesting
                        transformed_record[existing_field] = record[new_field]

            # Only add records that have mapped values
            if transformed_record:
                transformed_data.append(transformed_record)

        if not transformed_data:
            return jsonify({
                'message': 'No valid records after applying mappings',
                'status': 'error'
            }), 400

        # Backup current data
        backup_name = backup_sales_collection()

        # Get existing sales data
        existing_data = list(sales_collection.find())
        # Convert ObjectIds to strings for comparison
        for doc in existing_data:
            doc['_id'] = str(doc['_id'])

        # Merge the data based only on matching fields
        merged_data = merge_with_existing_data(existing_data, transformed_data, matching_fields)

        # Convert string IDs back to ObjectIds if they exist
        for doc in merged_data:
            if '_id' in doc and isinstance(doc['_id'], str):
                try:
                    doc['_id'] = ObjectId(doc['_id'])
                except:
                    # If conversion fails, remove the _id so MongoDB can assign a new one
                    del doc['_id']

        # Clear existing collection and insert merged data
        sales_collection.delete_many({})
        sales_collection.insert_many(merged_data)

        return jsonify({
            'message': 'Data merged successfully with field mappings',
            'status': 'success',
            'backup_name': backup_name,
            'record_count': len(merged_data)
        }), 200

    except Exception as e:
        return jsonify({
            'message': f'Error processing merge: {str(e)}',
            'status': 'error'
        }), 500

# Helper function to check if records match based on nested fields too
def records_match(record1, record2, matching_fields):
    for field in matching_fields:
        # Handle nested fields
        if '.' in field:
            parts = field.split('.')
            value1 = record1
            value2 = record2

            # Navigate to the nested value
            for part in parts:
                value1 = value1.get(part) if isinstance(value1, dict) else None
                value2 = value2.get(part) if isinstance(value2, dict) else None

            if value1 != value2:
                return False
        else:
            # Simple field comparison
            if record1.get(field) != record2.get(field):
                return False

    return True

# Helper function to merge data based on matching fields
# def merge_with_existing_data(existing_data, new_data, matching_fields):
#     merged_data = []
#     processed_existing_data = set()
#
#     # Process each record in the new data
#     for new_record in new_data:
#         match_found = False
#
#         # Check against existing data
#         for i, existing_record in enumerate(existing_data):
#             # Only check records we haven't processed yet
#             if i in processed_existing_data:
#                 continue
#
#             # Check if records match based on matching fields
#             is_match = True
#             for field in matching_fields:
#                 if field in existing_record and field in new_record:
#                     if existing_record[field] != new_record[field]:
#                         is_match = False
#                         break
#                 else:
#                     # If matching field doesn't exist in both records, not a match
#                     is_match = False
#                     break
#
#             if is_match:
#                 match_found = True
#                 processed_existing_data.add(i)
#
#                 # Merge the records, preferring values from new record
#                 merged_record = {**existing_record}
#                 for field, value in new_record.items():
#                     if value is not None:
#                         merged_record[field] = value
#
#                 merged_data.append(merged_record)
#                 break
#
#         # If no match found, add the new record as is
#         if not match_found:
#             merged_data.append(new_record)
#
#     # Add remaining existing records that weren't matched
#     for i, existing_record in enumerate(existing_data):
#         if i not in processed_existing_data:
#             merged_data.append(existing_record)
#
#     return merged_data


def merge_with_existing_data(existing_data, new_data, matching_fields):
    merged_data = []
    processed_existing_data = set()

    # Process each record in the new data
    for new_record in new_data:
        match_found = False

        # Check against existing data
        for i, existing_record in enumerate(existing_data):
            # Only check records we haven't processed yet
            if i in processed_existing_data:
                continue

            # Check if records match based on matching fields
            if records_match(existing_record, new_record, matching_fields):
                match_found = True
                processed_existing_data.add(i)

                # Create merged record by preserving structure
                merged_record = {**existing_record}

                # Update with new data at the field level
                for field, value in new_record.items():
                    if isinstance(value, dict) and field in merged_record and isinstance(merged_record[field], dict):
                        # For nested objects, merge recursively
                        merged_record[field] = {**merged_record[field], **value}
                    else:
                        # For simple fields or complete replacement of nested objects
                        merged_record[field] = value

                merged_data.append(merged_record)
                break

        # If no match found, add the new record as is
        if not match_found:
            merged_data.append(new_record)

    # Add remaining existing records that weren't matched
    for i, existing_record in enumerate(existing_data):
        if i not in processed_existing_data:
            merged_data.append(existing_record)

    return merged_data

# Update the existing sales-schema endpoint to include nested fields
@app.route('/api/sales-schema', methods=['GET'])
def get_sales_schema():
    sales = sales_collection.find_one()
    if sales:
        schema = get_schema_with_nested(sales)
        return jsonify({'schema': schema}), 200
    else:
        return jsonify({'schema': []}), 200  # No data in the collection

def get_schema_with_nested(document, prefix=""):
    """Extract schema including nested fields"""
    schema = []

    for key, value in document.items():
        # Skip MongoDB _id field
        if key == '_id':
            continue

        # Handle nested objects (except arrays)
        if isinstance(value, dict):
            nested_schema = get_schema_with_nested(value, f"{key}.")
            schema.extend(nested_schema)
        else:
            if prefix:
                schema.append(f"{prefix}{key}")
            else:
                schema.append(key)

    return schema


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


@app.route('/api/clear-database', methods=['GET'])
def clear_database():
    """
    Simple endpoint to clear all data in the sales collection.
    """
    try:
        # Get count of records before deletion
        record_count = sales_collection.count_documents({})

        # Clear the sales collection
        sales_collection.delete_many({})

        return jsonify({
            'message': f'Successfully cleared {record_count} records from the database',
            'deleted_count': record_count
        }), 200

    except Exception as e:
        return jsonify({
            'message': f'Error clearing database: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)