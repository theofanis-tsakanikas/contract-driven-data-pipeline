#!/bin/bash
set -e

echo "Waiting for Postgres to be ready..."
# This is a simple loop that checks if the Airflow database is ready by running 'airflow db check'.
until airflow db check > /dev/null 2>&1; do
  echo "Postgres not ready yet. Sleeping 5s..."
  sleep 5
done

echo "Initializing database..."
airflow db init
airflow db upgrade

echo "Ensuring admin user exists..."

# Use environment variables for admin user credentials, with defaults if not set
ADMIN_USER=${AIRFLOW_ADMIN_USER:-airflow}
ADMIN_PASS=${AIRFLOW_ADMIN_PASSWORD:-airflow}
ADMIN_EMAIL=${AIRFLOW_ADMIN_EMAIL:-airflow@example.com}

# This loop tries to create the admin user, but if it already exists, it will break out of the loop. 
for i in {1..5}; do # Try up to 5 times in case of transient issues
  if airflow users list | grep "$ADMIN_USER" > /dev/null 2>&1; then
    echo "User '$ADMIN_USER' already exists."
    break
  else
    echo "Creating user '$ADMIN_USER'..."
    airflow users create \
      --username "$ADMIN_USER" \
      --password "$ADMIN_PASS" \
      --firstname Airflow \
      --lastname Admin \
      --role Admin \
      --email "$ADMIN_EMAIL" && break || sleep 5
  fi
done

#
touch /opt/airflow/initialized
echo "Initialization complete."

# Keep container alive for healthcheck
exec tail -f /dev/null