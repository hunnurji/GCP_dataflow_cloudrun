from google.cloud import pubsub_v1
import json

class PubsubOperations:
    def __init__(self):
        try:
            self.publisher = pubsub_v1.PublisherClient()
            self.topic_path = self.publisher.topic_path("primal-duality-442608-r4", "vital-signs-topic")
        except Exception as e:
            raise Exception(f"Failed to initialize Pub/Sub client: {str(e)}")

    def publish_message(self, data):
        """
        Publishes a message to Google Cloud Pub/Sub.
        
        Args:
            data: Dictionary containing the message data
            
        Returns:
            dict: Status of the publish operation
            
        Raises:
            Exception: If message publishing fails
        """
        try:
            # Validate input
            if not isinstance(data, dict):
                raise ValueError("Input data must be a dictionary")

            # Prepare and publish message
            message_data = json.dumps(data).encode("utf-8")
            future = self.publisher.publish(self.topic_path, message_data)
            message_id = future.result()  # Wait for message to be published
            
            return {
                "status": "success",
                "message": "Message published successfully",
                "message_id": message_id
            }
            
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to encode message data: {str(e)}")
        except Exception as e:
            raise Exception(f"Failed to publish message: {str(e)}")

