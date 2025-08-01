#!/bin/sh

# Wait for Debezium Connect to be ready
echo "Waiting for Debezium Connect to be ready..."
until curl -f http://cdc_connect:8083/connectors; do
    echo "Connect not ready yet, waiting..."
    sleep 5
done

echo "Debezium Connect is ready. Checking PostgreSQL connector status..."

# Check if connector already exists
CONNECTOR_STATUS=$(curl -s http://cdc_connect:8083/connectors/postgres-connector/status 2>/dev/null)

if [ $? -eq 0 ] && echo "$CONNECTOR_STATUS" | grep -q '"state":"RUNNING"'; then
    echo "✅ PostgreSQL connector already exists and is running!"
    echo "Connector status: $(echo "$CONNECTOR_STATUS" | grep -o '"state":"[^"]*"')"
    exit 0
fi

echo "📡 PostgreSQL connector not found or not running. Creating connector..."

# Wait a bit more to ensure Connect is fully initialized
sleep 10

# Create PostgreSQL connector
RESPONSE=$(curl -s -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
http://cdc_connect:8083/connectors/ -d '{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "cdc_postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "sourcedb",
    "topic.prefix": "postgres",
    "table.include.list": "public.users",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot"
  }
}')

if echo "$RESPONSE" | grep -q "HTTP/1.1 201"; then
    echo "✅ PostgreSQL connector created successfully!"
elif echo "$RESPONSE" | grep -q "already exists"; then
    echo "ℹ️  PostgreSQL connector already exists."
else
    echo "❌ Failed to create connector. Response:"
    echo "$RESPONSE"
    exit 1
fi

echo "🎉 Connector setup completed!"
