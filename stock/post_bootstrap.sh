#!/bin/bash

echo "POST_BOOTSTRAP: Starting post-bootstrap script for stock database"

# Create replicator user for replication
echo "POST_BOOTSTRAP: Creating replicator user..."
psql -U postgres -c "CREATE USER replicator WITH REPLICATION PASSWORD 'replpassword';" 2>/dev/null || echo "User replicator already exists"

# Create application user
echo "POST_BOOTSTRAP: Creating application user..."
psql -U postgres -c "CREATE USER \"user\" WITH LOGIN PASSWORD 'password' CREATEROLE CREATEDB;" 2>/dev/null || echo "User 'user' already exists"

# Create database (might already exist from Patroni bootstrap)
echo "POST_BOOTSTRAP: Creating stock database..."
psql -U postgres -c "CREATE DATABASE stock OWNER \"user\";" 2>/dev/null || echo "Database stock already exists"

# Grant privileges to user
echo "POST_BOOTSTRAP: Setting up user permissions..."
psql -U postgres -d stock -c "GRANT ALL PRIVILEGES ON DATABASE stock TO \"user\";"

# Run init.sql
echo "POST_BOOTSTRAP: Running init.sql..."
psql -U postgres -d stock -f /tmp/init.sql

# Grant table privileges
echo "POST_BOOTSTRAP: Granting table privileges..."
psql -U postgres -d stock -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"user\";"
psql -U postgres -d stock -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"user\";"

echo "POST_BOOTSTRAP: Completed successfully"
exit 0