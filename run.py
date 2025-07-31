"""
CDC Pipeline Runner
Main script to run the CDC consumer
"""
import sys
import logging
from src.consumer import CDCProcessor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CDCPipelineRunner:
    """Manages the CDC consumer execution"""
    
    def __init__(self):
        self.consumer = CDCProcessor()
    
    def start_consumer(self) -> None:
        """Start the CDC consumer"""
        logger.info("Starting CDC Consumer...")
        try:
            self.consumer.start_processing()
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer failed: {e}")
            raise
    
    def show_status(self) -> None:
        """Display pipeline information"""
        print("\n" + "="*50)
        print("CDC PIPELINE INFO")
        print("="*50)
        print("Expected services (must be running externally):")
        print("- PostgreSQL (source): localhost:5432")
        print("- MySQL (target): localhost:3306")
        print("- Kafka: localhost:9092")
        print("- Debezium Connect: localhost:8083")
        print()
        print("Architecture: PostgreSQL → Debezium → Kafka → Python Consumer → MySQL")
        print("="*50)

def main():
    """Main entry point"""
    runner = CDCPipelineRunner()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "start":
            runner.start_consumer()
            
        elif command == "status":
            runner.show_status()
            
        else:
            print("Usage: python run.py [start|status]")
            sys.exit(1)
    else:
        # Default action is start
        runner.start_consumer()

if __name__ == "__main__":
    main()
