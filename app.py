from flask import Flask, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# Connect to MongoDB running in Docker
client = MongoClient("mongo", 27017)  # "mongo" is the service name from docker-compose.yml
db = client.mydatabase

@app.route('/')
def index():
    return jsonify(message="Hello from Flask with MongoDB!")

if __name__ == '__main__':
    # Listen on all interfaces, port 5000
    app.run(host='0.0.0.0', port=5000)
