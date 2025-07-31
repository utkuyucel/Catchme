#!/usr/bin/env python3
"""Quick CDC Test - Tests the complete CDC pipeline"""

import subprocess
import time
import sys

def run_command(cmd, description):
    """Run a command and show result"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ {description} - FAILED")
            if result.stderr:
                print(f"   Error: {result.stderr.strip()}")
            return False
    except subprocess.TimeoutExpired:
        print(f"⏰ {description} - TIMEOUT")
        return False
    except Exception as e:
        print(f"❌ {description} - ERROR: {e}")
        return False

def main():
    print("🧪 Quick CDC Pipeline Test")
    print("=" * 50)
    
    # Test 1: Check if Connect API is accessible
    connect_ok = run_command("curl -s -o /dev/null -w '%{http_code}' http://localhost:8083/", 
                             "Testing Connect API")
    
    # Test 2: Check PostgreSQL
    pg_ok = run_command("sudo docker compose exec -T postgres psql -U postgres -d sourcedb -c 'SELECT 1;'", 
                        "Testing PostgreSQL connection")
    
    # Test 3: Check MySQL  
    mysql_ok = run_command("sudo docker compose exec -T mysql mysql -u mysql -pmysql -e 'SELECT 1;'", 
                          "Testing MySQL connection")

    # Test 4: Insert test data into PostgreSQL
    if pg_ok:
        insert_ok = run_command(
            "sudo docker compose exec -T postgres psql -U postgres -d sourcedb -c \"INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');\"",
            "Inserting test data into PostgreSQL"
        )
    
    # Test 5: Check if data appears in MySQL (after a delay)
    if mysql_ok:
        print("⏳ Waiting 5 seconds for CDC to process...")
        time.sleep(5)
        mysql_check = run_command(
            "sudo docker compose exec -T mysql mysql -u mysql -pmysql targetdb -e \"SELECT * FROM users;\"",
            "Checking if data appeared in MySQL"
        )
    
    print("\n📊 Test Summary:")
    print(f"Connect API: {'✅' if connect_ok else '❌'}")
    print(f"PostgreSQL: {'✅' if pg_ok else '❌'}")  
    print(f"MySQL: {'✅' if mysql_ok else '❌'}")
    
    if all([connect_ok, pg_ok, mysql_ok]):
        print("\n🎉 Basic services are working!")
        print("\nTo manually test CDC:")
        print("1. Run: python3 run.py")
        print("2. In another terminal: sudo docker compose exec postgres psql -U postgres -d sourcedb")
        print("3. Insert data: INSERT INTO users (name, email) VALUES ('John', 'john@example.com');")
        print("4. Check MySQL: sudo docker compose exec mysql mysql -u mysql -pmysql targetdb -e 'SELECT * FROM users;'")
    else:
        print("\n⚠️ Some services have issues. Check Docker logs.")

if __name__ == "__main__":
    main()
