import json
import logging
import time
from typing import Dict, Any
from kafka import KafkaConsumer
from mysql.connector import connect, Error as MySQLError
from config import KafkaConfig, MySQLConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CDCProcessor:
    """Processes CDC events from Kafka and writes to MySQL"""
    
    def __init__(self):
        self.kafka_config = KafkaConfig()
        self.mysql_config = MySQLConfig()
        self.consumer = None
        self.mysql_conn = None
        
    def _init_kafka_consumer(self) -> KafkaConsumer:
        """Initialize Kafka consumer"""
        return KafkaConsumer(
            self.kafka_config.topic_name,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            group_id=self.kafka_config.group_id,
            auto_offset_reset=self.kafka_config.auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
    
    def _init_mysql_connection(self):
        """Initialize MySQL connection"""
        try:
            return connect(
                host=self.mysql_config.host,
                port=self.mysql_config.port,
                database=self.mysql_config.database,
                user=self.mysql_config.user,
                password=self.mysql_config.password
            )
        except MySQLError as e:
            logger.error("MySQL connection failed", extra={"error": str(e)})
            raise
    
    def _process_cdc_event(self, event: Dict[str, Any]) -> bool:
        """Process a single CDC event"""
        try:
            operation = event.get('payload', {}).get('op')
            before_data = event.get('payload', {}).get('before')
            after_data = event.get('payload', {}).get('after')
            
            if operation == 'c':  # CREATE
                return self._handle_insert(after_data)
            elif operation == 'u':  # UPDATE
                return self._handle_update(before_data, after_data)
            elif operation == 'd':  # DELETE
                return self._handle_delete(before_data)
            else:
                logger.warning("Unknown operation type", extra={"operation": operation})
                return False
                
        except Exception as e:
            logger.error("Error processing CDC event", extra={
                "error": str(e),
                "event": event
            })
            return False
    
    def _handle_insert(self, data: Dict[str, Any]) -> bool:
        """Handle INSERT operations"""
        if not data:
            return False
            
        try:
            cursor = self.mysql_conn.cursor()
            
            # Insert into users_cdc table
            query = """
                INSERT INTO users_cdc (user_id, name, email, operation, created_at)
                VALUES (%(id)s, %(name)s, %(email)s, 'INSERT', NOW())
            """
            cursor.execute(query, data)
            self.mysql_conn.commit()
            
            logger.info("Inserted CDC record", extra={
                "user_id": data.get('id'),
                "operation": "INSERT"
            })
            return True
            
        except MySQLError as e:
            logger.error("Insert operation failed", extra={
                "error": str(e),
                "data": data
            })
            self.mysql_conn.rollback()
            return False
        finally:
            cursor.close()
    
    def _handle_update(self, before: Dict[str, Any], after: Dict[str, Any]) -> bool:
        """Handle UPDATE operations"""
        if not after:
            return False
            
        try:
            cursor = self.mysql_conn.cursor()
            
            # Insert update record with before/after values
            query = """
                INSERT INTO users_cdc (user_id, name, email, operation, old_name, old_email, created_at)
                VALUES (%(id)s, %(name)s, %(email)s, 'UPDATE', %(old_name)s, %(old_email)s, NOW())
            """
            
            update_data = after.copy()
            update_data['old_name'] = before.get('name') if before else None
            update_data['old_email'] = before.get('email') if before else None
            
            cursor.execute(query, update_data)
            self.mysql_conn.commit()
            
            logger.info("Updated CDC record", extra={
                "user_id": after.get('id'),
                "operation": "UPDATE"
            })
            return True
            
        except MySQLError as e:
            logger.error("Update operation failed", extra={
                "error": str(e),
                "before": before,
                "after": after
            })
            self.mysql_conn.rollback()
            return False
        finally:
            cursor.close()
    
    def _handle_delete(self, data: Dict[str, Any]) -> bool:
        """Handle DELETE operations"""
        if not data:
            return False
            
        try:
            cursor = self.mysql_conn.cursor()
            
            query = """
                INSERT INTO users_cdc (user_id, name, email, operation, created_at)
                VALUES (%(id)s, %(name)s, %(email)s, 'DELETE', NOW())
            """
            cursor.execute(query, data)
            self.mysql_conn.commit()
            
            logger.info("Deleted CDC record", extra={
                "user_id": data.get('id'),
                "operation": "DELETE"
            })
            return True
            
        except MySQLError as e:
            logger.error("Delete operation failed", extra={
                "error": str(e),
                "data": data
            })
            self.mysql_conn.rollback()
            return False
        finally:
            cursor.close()
    
    def start_processing(self) -> None:
        """Start processing CDC events"""
        logger.info("Starting CDC processor")
        
        # Wait for services to be ready
        self._wait_for_services()
        
        # Initialize connections
        self.consumer = self._init_kafka_consumer()
        self.mysql_conn = self._init_mysql_connection()
        
        try:
            logger.info("Listening for CDC events", extra={
                "topic": self.kafka_config.topic_name,
                "group_id": self.kafka_config.group_id
            })
            
            for message in self.consumer:
                if message.value:
                    success = self._process_cdc_event(message.value)
                    if success:
                        logger.debug("Successfully processed CDC event")
                    else:
                        logger.warning("Failed to process CDC event")
                        
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down")
        except Exception as e:
            logger.error("Unexpected error in processing loop", extra={"error": str(e)})
        finally:
            self._cleanup()
    
    def _wait_for_services(self) -> None:
        """Wait for Kafka and MySQL to be ready"""
        max_retries = 30
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Test MySQL connection
                test_mysql = self._init_mysql_connection()
                test_mysql.close()
                
                # Test Kafka connection
                test_consumer = KafkaConsumer(
                    bootstrap_servers=self.kafka_config.bootstrap_servers,
                    consumer_timeout_ms=1000
                )
                test_consumer.close()
                
                logger.info("All services are ready")
                return
                
            except Exception as e:
                logger.info("Waiting for services to be ready", extra={
                    "attempt": attempt + 1,
                    "max_retries": max_retries,
                    "error": str(e)
                })
                time.sleep(retry_delay)
        
        raise RuntimeError("Services failed to become ready within timeout")
    
    def _cleanup(self) -> None:
        """Cleanup resources"""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
            
        if self.mysql_conn:
            self.mysql_conn.close()
            logger.info("MySQL connection closed")

def main():
    processor = CDCProcessor()
    processor.start_processing()

if __name__ == "__main__":
    main()
