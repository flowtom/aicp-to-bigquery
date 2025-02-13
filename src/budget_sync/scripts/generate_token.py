"""
Script to generate token.json file for Google OAuth2 authentication.
"""
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

def generate_token():
    # If modifying these scopes, delete the file token.json.
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    try:
        # Start the OAuth flow using credentials.json
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        
        # Run the local server flow
        creds = flow.run_local_server(port=0)
        
        # Save the credentials
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
        print("✅ Successfully generated token.json")
        
    except Exception as e:
        print(f"❌ Error generating token: {str(e)}")
        raise

if __name__ == "__main__":
    # Enable non-HTTPS local server for development
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    generate_token() 