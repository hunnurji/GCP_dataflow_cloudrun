from flask import Flask, request, jsonify
from datetime import datetime
from vitals_operations import PubsubOperations
from utils import ValidationUtils
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route('/ingest', methods=['POST'])
def ingest_vitals():
    """
    Endpoint to ingest patient vital signs data.
    Validates the data and publishes it to Pub/Sub.
    
    Returns:
        JSON response with status and message
    """
    try:
        # Validate request has JSON content
        if not request.is_json:
            raise ValueError("Request must contain JSON data")

        data = request.get_json()
        
        # Validate the incoming data
        is_valid, message = ValidationUtils.validate_vitals_data(data)
        if not is_valid:
            logger.warning(f"Invalid data received: {message}")
            raise ValueError(message)
        
        # Initialize PubsubOperations and publish message
        pubsub_ops = PubsubOperations()
        result = pubsub_ops.publish_message(data)
        
        if result["status"] != "success":
            logger.error(f"Failed to publish message: {result['message']}")
            raise Exception(f"Failed to process data: {result['message']}")
            
        logger.info(f"Successfully published message with ID: {result['message_id']}")
        return jsonify({
            "status": "success",
            "message": "Data ingested successfully",
            "message_id": result["message_id"]
        }), 200
            
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 400
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Internal server error",
            "details": str(e)
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
