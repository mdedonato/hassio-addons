#!/usr/bin/with-contenv bashio

echo Start Cummins Generator!

# Read options from Home Assistant addon config
MQTT_SERVER=$(bashio::config 'mqtt_server' '192.168.1.102')
MQTT_PORT=$(bashio::config 'mqtt_port' 1883)
MQTT_USERNAME=$(bashio::config 'mqtt_username' '')
MQTT_PASSWORD=$(bashio::config 'mqtt_password' '')
GENERATOR_ADDRESS=$(bashio::config 'generator_address' '192.168.1.6')
GENERATOR_USERNAME=$(bashio::config 'username' 'admin')
GENERATOR_PASSWORD=$(bashio::config 'password' 'cummins')
TIME_SYNC_MIN=$(bashio::config 'time_sync_min' 10)
MQTT_PREFIX=$(bashio::config 'mqtt_prefix' 'cummins/')
DEVICE_NAME=$(bashio::config 'device_name' 'Cummins Generator')
UNIQUE_ID=$(bashio::config 'unique_id' '')
DISCOVERY_PREFIX=$(bashio::config 'discovery_prefix' 'homeassistant')
LOG_LEVEL=$(bashio::config 'log_level' '')

# Create config.ini from options
cat > /config/config.ini <<EOF
[CUMMINS]
Host = ${GENERATOR_ADDRESS}
Username = ${GENERATOR_USERNAME}
Password = ${GENERATOR_PASSWORD}
TimeSyncMin = ${TIME_SYNC_MIN}

[MQTT]
Host = ${MQTT_SERVER}
Port = ${MQTT_PORT}
Username = ${MQTT_USERNAME}
Password = ${MQTT_PASSWORD}
Prefix = ${MQTT_PREFIX}
DeviceName = ${DEVICE_NAME}
DiscoveryPrefix = ${DISCOVERY_PREFIX}

[LOGGING]
LogLevel = ${LOG_LEVEL}
EOF

python cummins.py -d -c /config/config.ini

