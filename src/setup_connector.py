import requests
import json
import time
import logging
from config import DebeziumConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_debezium_connector():
    """Setup Debezium PostgreSQL connector"""
    config = DebeziumConfig()
    
    connector_config = {
        "name": config.connector_name,
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
            "slot.name": "debezium_slot",
            "publication.name": "dbz_publication",
            "publication.autocreate.mode": "filtered"
        }
    }
    
    # Wait for Connect to be ready
    max_retries = 30
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{config.connect_url}/connectors")
            if response.status_code == 200:
                logger.info("Debezium Connect is ready")
                break
        except requests.exceptions.RequestException:
            pass
        
        logger.info(f"Waiting for Connect to be ready... attempt {attempt + 1}/{max_retries}")
        time.sleep(2)
    else:
        raise RuntimeError("Connect failed to become ready")
    
    # Create connector
    try:
        response = requests.post(
            f"{config.connect_url}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config)
        )
        
        if response.status_code in [200, 201]:
            logger.info("Debezium connector created successfully")
        elif response.status_code == 409:
            logger.info("Debezium connector already exists")
        else:
            logger.error(f"Failed to create connector: {response.status_code} - {response.text}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating connector: {e}")

if __name__ == "__main__":
    setup_debezium_connector()
