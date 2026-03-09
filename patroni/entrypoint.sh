#!/bin/bash
set -e

echo "PATRONI ENTRYPOINT: Starting..."

# Ensure data directory has correct permissions
if [ -d /var/lib/postgresql/data ]; then
    chmod 700 /var/lib/postgresql/data 2>/dev/null || true
fi

# Substitute environment variables in the Patroni config
envsubst < /etc/patroni/patroni.yml > /tmp/patroni.yml

echo "PATRONI ENTRYPOINT: Config file created at /tmp/patroni.yml"
echo "PATRONI ENTRYPOINT: PATRONI_NAME = $PATRONI_NAME"

# Copy and make post_bootstrap script executable if it exists
if [ -f /docker-entrypoint-initdb.d/post_bootstrap.sh ]; then
    cp /docker-entrypoint-initdb.d/post_bootstrap.sh /tmp/post_bootstrap.sh
    chmod +x /tmp/post_bootstrap.sh
    echo "PATRONI ENTRYPOINT: post_bootstrap.sh copied to /tmp and made executable"
fi

# Copy init.sql if it exists
if [ -f /docker-entrypoint-initdb.d/init.sql ]; then
    cp /docker-entrypoint-initdb.d/init.sql /tmp/init.sql
    echo "PATRONI ENTRYPOINT: init.sql copied to /tmp"
fi

echo "PATRONI ENTRYPOINT: Starting Patroni..."

# Start Patroni
exec patroni /tmp/patroni.yml
