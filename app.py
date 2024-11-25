from flask import Flask, jsonify
from gcp_authenticator import GCPAuthenticator, Environment
from google.cloud import storage

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Required for session management

# Initialize GCP Authenticator
authenticator = GCPAuthenticator(
    project_id='your-project-id',
    environment=Environment.LOCAL,
    # Optionally specify service account file for local testing
    service_account_file='path/to/your/service-account.json'
)

@app.route('/')
def home():
    return jsonify({
        "status": "ok",
        "message": "GCP Authentication Test Server"
    })

@app.route('/test-auth')
@authenticator.requires_auth
def test_auth():
    try:
        # Test authentication by listing GCS buckets
        storage_client = storage.Client(credentials=authenticator.get_credentials())
        buckets = list(storage_client.list_buckets())
        
        return jsonify({
            "status": "success",
            "message": "Successfully authenticated",
            "buckets": [bucket.name for bucket in buckets]
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/auth-info')
@authenticator.requires_auth
def auth_info():
    credentials = authenticator.get_credentials()
    return jsonify({
        "status": "success",
        "auth_method": authenticator.auth_method.value,
        "environment": authenticator.environment.value,
        "project_id": authenticator.project_id,
        "credentials_type": str(type(credentials)),
        "is_valid": credentials.valid,
    })

# Initialize authentication before first request
@app.before_first_request
def before_first_request():
    authenticator.initialize_auth()

if __name__ == '__main__':
    app.run(debug=True, port=8080)