#!/usr/bin/env python3
"""
CDC Pipeline Test Script
Simple script to test the CDC pipeline components
"""
import requests
import json
import time
import subprocess

def test_connect_api():
    """Test if Debezium Connect is ready"""
    try:
        response = requests.get("http://localhost:8083/", timeout=5)
        if response.status_code == 200:
            print("✅ Debezium Connect is ready")
            return True
        else:
            print(f"❌ Connect responded with status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Connect not accessible: {e}")
        return False

def check_connectors():
    """Check existing connectors"""
    try:
        response = requests.get("http://localhost:8083/connectors", timeout=5)
        if response.status_code == 200:
            connectors = response.json()
            print(f"📋 Existing connectors: {connectors}")
            return connectors
        else:
            print(f"❌ Failed to get connectors: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error checking connectors: {e}")
        return []

def test_postgres_connection():
    """Test PostgreSQL connection"""
    try:
        result = subprocess.run([
            "sudo", "docker", "compose", "exec", "-T", "postgres", 
            "psql", "-U", "postgres", "-d", "sourcedb", "-c", "SELECT 1;"
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ PostgreSQL is accessible")
            return True
        else:
            print(f"❌ PostgreSQL error: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ PostgreSQL test failed: {e}")
        return False

def test_mysql_connection():
    """Test MySQL connection"""
    try:
        result = subprocess.run([
            "sudo", "docker", "compose", "exec", "-T", "mysql",
            "mysql", "-u", "mysql", "-pmysql", "-e", "SELECT 1;"
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ MySQL is accessible")
            return True
        else:
            print(f"❌ MySQL error: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ MySQL test failed: {e}")
        return False

def main():
    print("🧪 Testing CDC Pipeline Components")
    print("=" * 40)
    
    # Test services
    connect_ready = test_connect_api()
    postgres_ready = test_postgres_connection()
    mysql_ready = test_mysql_connection()
    
    if connect_ready:
        connectors = check_connectors()
    
    print("\n📊 Summary:")
    print(f"Connect API: {'✅' if connect_ready else '❌'}")
    print(f"PostgreSQL: {'✅' if postgres_ready else '❌'}")
    print(f"MySQL: {'✅' if mysql_ready else '❌'}")
    
    if all([connect_ready, postgres_ready, mysql_ready]):
        print("\n🎉 All components are ready! You can now test CDC.")
        print("\nNext steps:")
        print("1. Run: python3 run.py")
        print("2. In another terminal, insert data into PostgreSQL")
        print("3. Check if data appears in MySQL")
    else:
        print("\n⚠️  Some components are not ready. Wait a bit and try again.")

if __name__ == "__main__":
    main()
