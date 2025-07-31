#!/usr/bin/env python3
"""
Complete CDC Pipeline Test
Tests the full end-to-end CDC functionality
"""
import subprocess
import time
import sys
import threading
import signal

# Global flag to control the consumer
consumer_running = False
consumer_process = None

def run_consumer():
    """Run the CDC consumer in the background"""
    global consumer_process, consumer_running
    try:
        print("🚀 Starting CDC consumer...")
        consumer_process = subprocess.Popen(
            ["python3", "run.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        consumer_running = True
        
        # Read output
        for line in iter(consumer_process.stdout.readline, ''):
            if consumer_running:
                print(f"Consumer: {line.strip()}")
            else:
                break
                
    except Exception as e:
        print(f"❌ Consumer error: {e}")
        consumer_running = False

def stop_consumer():
    """Stop the CDC consumer"""
    global consumer_process, consumer_running
    consumer_running = False
    if consumer_process:
        consumer_process.terminate()
        try:
            consumer_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            consumer_process.kill()
        print("⏹️ Consumer stopped")

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    print("\n🛑 Stopping test...")
    stop_consumer()
    sys.exit(0)

def run_command(cmd, description, timeout=10):
    """Run a command with timeout"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, result.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)

def test_cdc_pipeline():
    """Test the complete CDC pipeline"""
    print("🧪 Complete CDC Pipeline Test")
    print("=" * 60)
    
    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Step 1: Start consumer in background
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()
    
    # Wait for consumer to initialize
    print("⏳ Waiting for consumer to initialize...")
    time.sleep(10)
    
    if not consumer_running:
        print("❌ Consumer failed to start")
        return False
    
    # Step 2: Insert test data into PostgreSQL
    print("\n📝 Inserting test data into PostgreSQL...")
    insert_cmd = """sudo docker compose exec -T postgres psql -U postgres -d sourcedb -c "INSERT INTO users (name, email) VALUES ('CDC Test User', 'cdc.test@example.com');" """
    
    success, output = run_command(insert_cmd, "Insert test data")
    if success:
        print("✅ Test data inserted successfully")
        print(f"   Output: {output}")
    else:
        print(f"❌ Failed to insert test data: {output}")
        stop_consumer()
        return False
    
    # Step 3: Wait for CDC processing
    print("\n⏳ Waiting for CDC to process the change...")
    for i in range(10):
        time.sleep(2)
        print(f"   Waiting... {i*2 + 2}s")
    
    # Step 4: Check if data appeared in MySQL
    print("\n🔍 Checking if data appeared in MySQL...")
    check_cmd = """sudo docker compose exec -T mysql mysql -u mysql -pmysql targetdb -e "SELECT * FROM users WHERE email = 'cdc.test@example.com';" """
    
    success, output = run_command(check_cmd, "Check MySQL data")
    if success and "cdc.test@example.com" in output:
        print("🎉 SUCCESS! CDC pipeline is working!")
        print("   Data successfully replicated from PostgreSQL to MySQL")
        print(f"   MySQL output:\n{output}")
        result = True
    else:
        print("❌ CDC pipeline test failed")
        print(f"   MySQL output: {output}")
        
        # Check if table exists
        table_cmd = """sudo docker compose exec -T mysql mysql -u mysql -pmysql targetdb -e "SHOW TABLES;" """
        table_success, table_output = run_command(table_cmd, "Check tables")
        print(f"   Available tables: {table_output}")
        result = False
    
    # Step 5: Cleanup
    print("\n🧹 Cleaning up...")
    stop_consumer()
    
    return result

if __name__ == "__main__":
    success = test_cdc_pipeline()
    if success:
        print("\n✅ CDC Pipeline Test PASSED")
        print("\nYour CDC pipeline is working correctly!")
        print("You can now:")
        print("1. Run: python3 run.py")
        print("2. Insert/update/delete data in PostgreSQL")
        print("3. Watch it appear in MySQL automatically")
    else:
        print("\n❌ CDC Pipeline Test FAILED")
        print("Check the logs and configuration")
    
    sys.exit(0 if success else 1)
