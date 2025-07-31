"""
CDC Pipeline Runner
Main script to start and manage the entire CDC pipeline
"""
import subprocess
import time
import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CDCPipelineRunner:
    """Manages the complete CDC pipeline lifecycle"""
    
    def __init__(self):
        self.compose_file = Path("docker-compose.yml")
        self.services_ready_timeout = 60
        self.connector_setup_timeout = 30
        
    def _run_command(self, command: list, check: bool = True) -> subprocess.CompletedProcess:
        """Execute shell command with error handling"""
        try:
            logger.info(f"Running: {' '.join(command)}")
            result = subprocess.run(
                command,
                check=check,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.stdout:
                logger.info(f"Output: {result.stdout.strip()}")
            if result.stderr and result.returncode != 0:
                logger.error(f"Error: {result.stderr.strip()}")
                
            return result
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e}")
            logger.error(f"Return code: {e.returncode}")
            logger.error(f"Error output: {e.stderr}")
            raise
        except subprocess.TimeoutExpired:
            logger.error("Command timed out")
            raise
    
    def start_services(self) -> bool:
        """Start Docker Compose services"""
        logger.info("Starting CDC Pipeline Setup...")
        
        if not self.compose_file.exists():
            logger.error("docker-compose.yml not found")
            return False
        
        try:
            logger.info("Starting Docker Compose services...")
            self._run_command(["docker-compose", "up", "-d"])
            logger.info("Services started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start services: {e}")
            return False
    
    def wait_for_services(self) -> bool:
        """Wait for all services to be ready"""
        logger.info(f"Waiting for services to be ready (timeout: {self.services_ready_timeout}s)...")
        
        start_time = time.time()
        while time.time() - start_time < self.services_ready_timeout:
            try:
                # Check if all services are running
                result = self._run_command(
                    ["docker-compose", "ps", "--services", "--filter", "status=running"],
                    check=False
                )
                
                if result.returncode == 0:
                    running_services = result.stdout.strip().split('\n')
                    expected_services = ['postgres', 'mysql', 'zookeeper', 'kafka', 'connect', 'consumer']
                    
                    if all(service in running_services for service in expected_services):
                        logger.info("All services are running")
                        # Additional wait for service initialization
                        time.sleep(10)
                        return True
                
                logger.info("Services still starting up...")
                time.sleep(5)
                
            except Exception as e:
                logger.warning(f"Error checking services: {e}")
                time.sleep(5)
        
        logger.error("Services failed to start within timeout")
        return False
    
    def setup_debezium_connector(self) -> bool:
        """Setup Debezium connector via the consumer container"""
        logger.info("Setting up Debezium connector...")
        
        try:
            # Wait a bit more for Connect to be fully ready
            time.sleep(10)
            
            result = self._run_command([
                "docker-compose", "exec", "-T", "consumer",
                "python", "-m", "src.setup_connector"
            ])
            
            if result.returncode == 0:
                logger.info("Debezium connector setup completed")
                return True
            else:
                logger.error("Debezium connector setup failed")
                return False
                
        except Exception as e:
            logger.error(f"Failed to setup Debezium connector: {e}")
            return False
    
    def show_status(self) -> None:
        """Display pipeline status and usage information"""
        logger.info("CDC Pipeline is ready!")
        print("\n" + "="*50)
        print("CDC PIPELINE STATUS")
        print("="*50)
        print("Services running:")
        print("- PostgreSQL (source): localhost:5432")
        print("- MySQL (target): localhost:3306")
        print("- Kafka: localhost:9092")
        print("- Debezium Connect: localhost:8083")
        print()
        print("Manual testing commands:")
        print("PostgreSQL: docker-compose exec postgres psql -U postgres -d sourcedb")
        print("MySQL: docker-compose exec mysql mysql -u mysql -pmysql targetdb")
        print("Logs: docker-compose logs -f consumer")
        print()
        print("Architecture: PostgreSQL → Debezium → Kafka → Python Consumer → MySQL")
        print("="*50)
    
    def stop_services(self) -> bool:
        """Stop all services"""
        logger.info("Stopping CDC pipeline...")
        
        try:
            self._run_command(["docker-compose", "down", "-v"])
            logger.info("CDC pipeline stopped")
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop services: {e}")
            return False
    
    def start_pipeline(self) -> bool:
        """Start the complete CDC pipeline"""
        steps = [
            ("Starting services", self.start_services),
            ("Waiting for services", self.wait_for_services),
            ("Setting up Debezium", self.setup_debezium_connector)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"Step: {step_name}")
            if not step_func():
                logger.error(f"Failed at step: {step_name}")
                return False
        
        self.show_status()
        return True

def main():
    """Main entry point"""
    runner = CDCPipelineRunner()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "start":
            success = runner.start_pipeline()
            sys.exit(0 if success else 1)
            
        elif command == "stop":
            success = runner.stop_services()
            sys.exit(0 if success else 1)
            
        elif command == "status":
            runner.show_status()
            sys.exit(0)
            
        else:
            print("Usage: python run.py [start|stop|status]")
            sys.exit(1)
    else:
        # Default action is start
        success = runner.start_pipeline()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
