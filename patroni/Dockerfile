FROM postgres:16

# Install dependencies for Patroni
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-psycopg2 \
    python3-yaml \
    python3-urllib3 \
    python3-requests \
    gettext-base \
    && rm -rf /var/lib/apt/lists/*

# Install Patroni with etcd support
RUN pip3 install --break-system-packages patroni[etcd3]

# Create necessary directories with proper permissions
RUN mkdir -p /var/lib/postgresql/data && \
    chmod 700 /var/lib/postgresql/data && \
    chown -R postgres:postgres /var/lib/postgresql && \
    mkdir -p /var/run/postgresql && \
    chmod 755 /var/run/postgresql && \
    chown -R postgres:postgres /var/run/postgresql && \
    mkdir -p /tmp && \
    chmod 1777 /tmp

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER postgres

EXPOSE 5432 8008

ENTRYPOINT ["/entrypoint.sh"]
