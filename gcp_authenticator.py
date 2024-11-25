from typing import Optional, Dict, Any, Union
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import base64
from enum import Enum

from google.oauth2 import service_account, credentials
from google.oauth2.credentials import Credentials
from google.auth.transport import requests
from google.auth import default, compute_engine, impersonated_credentials
from google.cloud import storage, secretmanager
from flask import session, redirect, url_for, request
import google_auth_oauthlib.flow
from functools import wraps

class AuthMethod(Enum):
    """Enumeration of supported authentication methods"""
    SERVICE_ACCOUNT = "service_account"
    OAUTH = "oauth"
    ADC = "adc"
    WORKLOAD_IDENTITY = "workload_identity"
    METADATA_SERVER = "metadata_server"
    SECRET_MANAGER = "secret_manager"

class Environment(Enum):
    """Enumeration of deployment environments"""
    LOCAL = "local"
    CLOUD_RUN = "cloud_run"
    GKE = "gke"
    APP_ENGINE = "app_engine"
    COMPUTE_ENGINE = "compute_engine"
    CLOUD_FUNCTIONS = "cloud_functions"

class GCPAuthenticator:
    """
    A comprehensive GCP authentication handler for Flask applications,
    with support for various deployment environments.
    """
    
    def __init__(self, 
                 project_id: str,
                 environment: Union[Environment, str] = Environment.LOCAL,
                 auth_method: Union[AuthMethod, str] = None,
                 client_secrets_file: Optional[str] = None,
                 service_account_file: Optional[str] = None):
        """
        Initialize the GCP authenticator.
        
        Args:
            project_id: GCP project ID
            environment: Deployment environment
            auth_method: Authentication method to use
            client_secrets_file: Path to OAuth 2.0 client secrets file
            service_account_file: Path to service account key file
        """
        self.project_id = project_id
        self.environment = Environment(environment) if isinstance(environment, str) else environment
        self.auth_method = AuthMethod(auth_method) if isinstance(auth_method, str) and auth_method else None
        self.client_secrets_file = client_secrets_file
        self.service_account_file = service_account_file
        self.credentials = None
        self._token_cache = {}
        
        # OAuth2 configuration
        self.oauth_scopes = [
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/userinfo.email'
        ]

        # Auto-detect environment and auth method if not specified
        if not self.auth_method:
            self.auth_method = self._detect_auth_method()

    def _detect_auth_method(self):
        """
        Automatically detect the appropriate authentication method
        based on the deployment environment.
        """
        # Check for GCP environment variables
        if os.getenv('K_SERVICE'):  # Cloud Run
            self.environment = Environment.CLOUD_RUN
            return AuthMethod.WORKLOAD_IDENTITY
        elif os.getenv('KUBERNETES_SERVICE_HOST'):  # GKE
            self.environment = Environment.GKE
            return AuthMethod.WORKLOAD_IDENTITY
        elif os.getenv('GAE_APPLICATION'):  # App Engine
            self.environment = Environment.APP_ENGINE
            return AuthMethod.METADATA_SERVER
        elif os.getenv('FUNCTION_NAME'):  # Cloud Functions
            self.environment = Environment.CLOUD_FUNCTIONS
            return AuthMethod.WORKLOAD_IDENTITY
        elif os.getenv('COMPUTE_ENGINE'):  # Compute Engine
            self.environment = Environment.COMPUTE_ENGINE
            return AuthMethod.METADATA_SERVER
        
        # Local development
        return AuthMethod.SERVICE_ACCOUNT

    def initialize_auth(self):
        """
        Initialize authentication based on the detected or specified method.
        """
        try:
            if self.auth_method == AuthMethod.WORKLOAD_IDENTITY:
                self._init_workload_identity()
            elif self.auth_method == AuthMethod.METADATA_SERVER:
                self._init_metadata_server()
            elif self.auth_method == AuthMethod.SECRET_MANAGER:
                self._init_from_secret_manager()
            elif self.auth_method == AuthMethod.SERVICE_ACCOUNT:
                self._init_service_account()
            elif self.auth_method == AuthMethod.OAUTH:
                # OAuth initialization is handled separately through init_oauth_flow
                pass
            elif self.auth_method == AuthMethod.ADC:
                self._init_adc()
            else:
                raise ValueError(f"Unsupported authentication method: {self.auth_method}")
        except Exception as e:
            raise Exception(f"Failed to initialize authentication: {str(e)}")

    def _init_workload_identity(self):
        """Initialize authentication using Workload Identity"""
        try:
            self.credentials, project = default()
        except Exception as e:
            raise Exception(f"Failed to initialize Workload Identity: {str(e)}")

    def _init_metadata_server(self):
        """Initialize authentication using GCP metadata server"""
        try:
            self.credentials = compute_engine.Credentials()
        except Exception as e:
            raise Exception(f"Failed to initialize metadata server authentication: {str(e)}")

    def _init_from_secret_manager(self):
        """Initialize authentication using credentials stored in Secret Manager"""
        try:
            # First, get default credentials to access Secret Manager
            default_credentials, _ = default()
            
            # Create Secret Manager client
            client = secretmanager.SecretManagerServiceClient(
                credentials=default_credentials
            )
            
            # Construct the secret name
            secret_name = f"projects/{self.project_id}/secrets/service-account-key/versions/latest"
            
            # Access the secret
            response = client.access_secret_version(request={"name": secret_name})
            secret_content = response.payload.data.decode("UTF-8")
            
            # Initialize service account with the secret content
            service_account_info = json.loads(secret_content)
            self.credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=self.oauth_scopes
            )
        except Exception as e:
            raise Exception(f"Failed to initialize from Secret Manager: {str(e)}")

    def _init_service_account(self):
        """Initialize service account authentication"""
        try:
            # Check environment variables first
            sa_json_str = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
            if sa_json_str:
                # Decode if base64 encoded
                try:
                    sa_json_str = base64.b64decode(sa_json_str).decode('utf-8')
                except:
                    pass  # Not base64 encoded
                service_account_info = json.loads(sa_json_str)
            elif self.service_account_file and os.path.exists(self.service_account_file):
                with open(self.service_account_file, 'r') as f:
                    service_account_info = json.load(f)
            else:
                raise ValueError("No service account credentials found")

            self.credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=self.oauth_scopes
            )
        except Exception as e:
            raise Exception(f"Failed to initialize service account: {str(e)}")

    def _init_adc(self):
        """Initialize Application Default Credentials"""
        try:
            self.credentials, project = default()
        except Exception as e:
            raise Exception(f"Failed to initialize ADC: {str(e)}")

    def get_credentials(self):
        """
        Get the current credentials.
        
        Returns:
            Active credentials object
        """
        if not self.credentials:
            raise ValueError("No credentials initialized. Call initialize_auth() first.")
        return self.credentials

    def requires_auth(self, f):
        """Decorator to require authentication for Flask routes."""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not self.credentials:
                if self.environment == Environment.LOCAL and 'credentials' in session:
                    self.credentials = Credentials(**session['credentials'])
                else:
                    self.initialize_auth()
                    
            if not self.credentials.valid:
                if self.credentials.expired and self.credentials.refresh_token:
                    self.credentials.refresh(requests.Request())
                    
                    if self.environment == Environment.LOCAL:
                        session['credentials'] = {
                            'token': self.credentials.token,
                            'refresh_token': self.credentials.refresh_token,
                            'token_uri': self.credentials.token_uri,
                            'client_id': self.credentials.client_id,
                            'client_secret': self.credentials.client_secret,
                            'scopes': self.credentials.scopes
                        }
            return f(*args, **kwargs)
        return decorated_function

    @staticmethod
    def get_deployment_instructions():
        """
        Get deployment-specific authentication instructions.
        """
        return """
        Deployment Authentication Instructions:
        
        1. Cloud Run:
            - Use Workload Identity
            - Set IAM roles for the Cloud Run service account
            
        2. GKE:
            - Use Workload Identity
            - Configure Workload Identity for your GKE cluster
            - Annotate service account appropriately
            
        3. App Engine:
            - Uses built-in service account
            - Configure service account permissions via IAM
            
        4. Cloud Functions:
            - Uses built-in service account
            - Configure service account permissions via IAM
            
        5. Compute Engine:
            - Use attached service account
            - Configure instance service account permissions
            
        For local development:
        1. Use ADC: `gcloud auth application-default login`
        2. Or set GOOGLE_APPLICATION_CREDENTIALS environment variable
        3. Or use service account key (not recommended for production)
        """