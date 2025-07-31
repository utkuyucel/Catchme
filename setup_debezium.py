#!/usr/bin/env python3
"""
Setup Debezium Connector via REST API
"""
import requests
import json
import time

# Debezium connector configuration
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "sourcedb",
        "database.server.name": "postgres",
        "table.include.list": "public.users",
        "plugin.name": "pgoutput",
        "publication.autocreate.mode": "filtered",
        "topic.prefix": "postgres"
    }
}

def setup_connector():
    """Setup the Debezium connector"""
    connect_url = "http://localhost:8083"
    
    # Wait for Connect to be ready
    print("⏳ Waiting for Connect to be ready...")
    for i in range(30):
        try:
            response = requests.get(f"{connect_url}/", timeout=5)
            if response.status_code == 200:
                print("✅ Connect is ready!")
                break
        except:
            pass
        time.sleep(2)
        print(f"   Attempt {i+1}/30...")
    else:
        print("❌ Connect not ready after 60 seconds")
        return False
    
    # Check existing connectors
    try:
        response = requests.get(f"{connect_url}/connectors")
        existing_connectors = response.json()
        print(f"📋 Existing connectors: {existing_connectors}")
        
        if "postgres-connector" in existing_connectors:
            print("🔄 Deleting existing connector...")
            requests.delete(f"{connect_url}/connectors/postgres-connector")
            time.sleep(2)
    except Exception as e:
        print(f"⚠️ Error checking connectors: {e}")
    
    # Create the connector
    try:
        print("🚀 Creating Debezium connector...")
        response = requests.post(
            f"{connect_url}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config)
        )
        
        if response.status_code in [200, 201]:
            print("✅ Connector created successfully!")
            print(response.json())
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {e}")
        return False

if __name__ == "__main__":
    setup_connector()
