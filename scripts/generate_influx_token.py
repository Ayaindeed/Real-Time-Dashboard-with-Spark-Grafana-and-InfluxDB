#!/usr/bin/env python3
"""
Generate InfluxDB API Token for Grafana
"""

import requests
import json
import sys

def generate_influxdb_token():
    """Generate an API token for InfluxDB"""
    
    # InfluxDB connection details
    influxdb_url = "http://localhost:8086"
    username = "admin"
    password = "password123"
    org = "ecommerce-org"
    
    print("Generating InfluxDB API Token...")
    
    try:
        # First, we need to sign in to get a session token
        signin_url = f"{influxdb_url}/api/v2/signin"
        signin_data = {
            "username": username,
            "password": password
        }
        
        # Sign in
        response = requests.post(signin_url, json=signin_data)
        if response.status_code != 204:
            print(f"Failed to sign in: {response.status_code}")
            print(response.text)
            return None
        
        # Get session cookie
        session_cookie = response.cookies.get('influxdb-oss-session')
        if not session_cookie:
            print("No session cookie received")
            return None
        
        # Create authorization/token
        auth_url = f"{influxdb_url}/api/v2/authorizations"
        
        # Token permissions
        token_data = {
            "status": "active",
            "description": "Grafana Read Token",
            "orgID": None,  # We'll get this from the org
            "permissions": [
                {
                    "action": "read",
                    "resource": {
                        "type": "buckets"
                    }
                },
                {
                    "action": "read", 
                    "resource": {
                        "type": "dashboards"
                    }
                },
                {
                    "action": "read",
                    "resource": {
                        "type": "tasks"
                    }
                },
                {
                    "action": "read",
                    "resource": {
                        "type": "telegrafs"
                    }
                },
                {
                    "action": "read",
                    "resource": {
                        "type": "users"
                    }
                },
                {
                    "action": "read",
                    "resource": {
                        "type": "variables"
                    }
                },
                {
                    "action": "read",
                    "resource": {
                        "type": "secrets"
                    }
                },
                {
                    "action": "read",
                    "resource": {
                        "type": "labels"
                    }
                },
                {
                    "action": "read",
                    "resource": {
                        "type": "views"
                    }
                },
                {
                    "action": "read",
                    "resource": {
                        "type": "documents"
                    }
                }
            ]
        }
        
        # Get organization ID first
        orgs_url = f"{influxdb_url}/api/v2/orgs"
        cookies = {'influxdb-oss-session': session_cookie}
        
        orgs_response = requests.get(orgs_url, cookies=cookies)
        if orgs_response.status_code == 200:
            orgs_data = orgs_response.json()
            org_id = None
            for org_info in orgs_data.get('orgs', []):
                if org_info.get('name') == org:
                    org_id = org_info.get('id')
                    break
            
            if org_id:
                token_data['orgID'] = org_id
            else:
                print(f"Organization '{org}' not found")
                return None
        else:
            print(f"Failed to get organizations: {orgs_response.status_code}")
            return None
        
        # Create the token
        auth_response = requests.post(auth_url, json=token_data, cookies=cookies)
        
        if auth_response.status_code == 201:
            auth_result = auth_response.json()
            token = auth_result.get('token')
            
            print("\n" + "="*60)
            print("SUCCESS! InfluxDB API Token Generated")
            print("="*60)
            print(f"Token: {token}")
            print("\nGrafana InfluxDB Data Source Configuration:")
            print(f"  URL: http://influxdb:8086")
            print(f"  Organization: {org}")
            print(f"  Token: {token}")
            print(f"  Default Bucket: events")
            print("="*60)
            
            return token
        else:
            print(f"Failed to create token: {auth_response.status_code}")
            print(auth_response.text)
            return None
            
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to InfluxDB")
        print("Make sure InfluxDB is running on http://localhost:8086")
        return None
    except Exception as e:
        print(f"Error generating token: {e}")
        return None

if __name__ == "__main__":
    token = generate_influxdb_token()
    if not token:
        sys.exit(1)