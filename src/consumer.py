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
        def safe_deserializer(message_bytes):
            if message_bytes is None:
                return None
            try:
                return json.loads(message_bytes.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Failed to deserialize message: {e}")
                return None
        
        return KafkaConsumer(
            self.kafka_config.topic_name,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            group_id=self.kafka_config.group_id,
            auto_offset_reset=self.kafka_config.auto_offset_reset,
            value_deserializer=safe_deserializer,
            consumer_timeout_ms=None  # No timeout - keep listening indefinitely
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
            if not isinstance(event, dict):
                logger.warning("Event is not a dictionary", extra={"event_type": type(event)})
                return False
                
            payload = event.get('payload', {})
            if not payload:
                logger.warning("No payload in event", extra={"event": event})
                return False
                
            operation = payload.get('op')
            before_data = payload.get('before')
            after_data = payload.get('after')
            
            if operation == 'c':  # CREATE (INSERT)
                return self._handle_insert(after_data)
            elif operation == 'u':  # UPDATE
                return self._handle_update(before_data, after_data)
            elif operation == 'd':  # DELETE
                return self._handle_delete(before_data)
            elif operation == 'r':  # READ (snapshot)
                return self._handle_insert(after_data)  # Treat snapshot reads as inserts
            else:
                logger.warning("Unknown operation type", extra={"operation": operation})
                return False
                
        except Exception as e:
            logger.error("Error processing CDC event", extra={
                "error": str(e),
                "event": str(event)[:200] if event else "None"
            })
            return False
    
    def _handle_insert(self, data: Dict[str, Any]) -> bool:
        """Handle INSERT operations"""
        if not data:
            return False
            
        try:
            cursor = self.mysql_conn.cursor()
            
            # Direct INSERT into users table with proper timestamp conversion
            query = """
                INSERT INTO users (id, name, email, created_at, updated_at)
                VALUES (%(id)s, %(name)s, %(email)s, 
                        FROM_UNIXTIME(%(created_at)s/1000000), 
                        FROM_UNIXTIME(%(updated_at)s/1000000))
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                email = VALUES(email),
                updated_at = VALUES(updated_at)
            """
            
            cursor.execute(query, data)
            self.mysql_conn.commit()
            
            logger.info("Inserted/Updated user record", extra={
                "user_id": data.get('id'),
                "name": data.get('name'),
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
            
            # Direct UPDATE to users table
            query = """
                UPDATE users 
                SET name = %(name)s, 
                    email = %(email)s, 
                    updated_at = FROM_UNIXTIME(%(updated_at)s/1000000)
                WHERE id = %(id)s
            """
            
            cursor.execute(query, after)
            self.mysql_conn.commit()
            
            if cursor.rowcount == 0:
                # If no rows updated, try insert (in case record doesn't exist)
                logger.info("No rows updated, attempting insert")
                return self._handle_insert(after)
            
            logger.info("Updated user record", extra={
                "user_id": after.get('id'),
                "name": after.get('name'),
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
            
            # Direct DELETE from users table
            query = "DELETE FROM users WHERE id = %(id)s"
            cursor.execute(query, data)
            self.mysql_conn.commit()
            
            logger.info("Deleted user record", extra={
                "user_id": data.get('id'),
                "name": data.get('name'),
                "operation": "DELETE",
                "rows_affected": cursor.rowcount
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
        
        try:
            # Initialize connections
            self.consumer = self._init_kafka_consumer()
            self.mysql_conn = self._init_mysql_connection()
            
            logger.info("Listening for CDC events")
            
            # Process messages using polling method
            while True:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Skip None values (tombstone records or malformed messages)
                            if message.value is None:
                                logger.debug("Skipping null/malformed message")
                                continue
                                
                            logger.info("Received CDC event", extra={"event": str(message.value)[:200]})  # Truncate for logging
                            
                            if self._process_cdc_event(message.value):
                                logger.info("Successfully processed CDC event")
                            else:
                                logger.warning("Failed to process CDC event")
                                
                        except Exception as e:
                            logger.error("Error processing message", extra={
                                "error": str(e),
                                "message": str(message.value)[:200] if message.value else "None"
                            })
                            
        except KeyboardInterrupt:
            logger.info("CDC processor stopped by user")
        except Exception as e:
            logger.error("CDC processor failed", extra={"error": str(e), "error_type": type(e).__name__})
            import traceback
            logger.error("Full traceback", extra={"traceback": traceback.format_exc()})
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self) -> None:
        """Clean up resources"""
        if self.consumer:
            logger.info("Closing Kafka consumer")
            self.consumer.close()
            
        if self.mysql_conn:
            logger.info("Closing MySQL connection")
            self.mysql_conn.close()

if __name__ == "__main__":
    processor = CDCProcessor()
    processor.start_processing()
