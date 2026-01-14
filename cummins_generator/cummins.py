"""
Cummins Generator Monitor and Controller

Monitors a Cummins generator via HTTP and publishes status to MQTT.
Supports remote control commands via MQTT subscriptions.
"""

__version__ = "1.1.0"

import re
import requests
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import argparse
import threading
import configparser
import pytz
from tzlocal import get_localzone
import signal
import logging
import sys
import json
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None  # Will handle gracefully


# Constants
TIMEZONE = pytz.timezone('America/New_York')
REQUEST_TIMEOUT = 10
MQTT_QOS = 0
MQTT_RETAIN = True

# Status code mappings
STATUS_CODES = {
    0: "Stopped",
    1: "Stopped",
    2: "Starting",
    3: "Starting",
    4: "Running",
    5: "Priming",
    6: "Fault",  # Will append fault number
    7: "Eng.Only",
    8: "TestMode",
    9: "Volt Adj",
    20: "** Config Mode **",
    21: "Cycle crank pause",
    22: "Exercising",
    23: "Engine Cooldown",
}

# LCD Status bit masks
LCD_BIT_UTILITY_PRESENT = 0x01
LCD_BIT_UTILITY_CONNECTED = 0x02
LCD_BIT_RUNNING = 0x0C
LCD_BIT_STANDBY = 0x10
LCD_BIT_ACTION_REQUIRED = 0x60

# Generator control parameters
PARAM_STANDBY_DISABLE = '@385=0'
PARAM_STANDBY_ENABLE = '@385=1'
PARAM_ENGINE_STOP = '@242=1'
PARAM_ENGINE_START = '@242=2'
PARAM_ENGINE_EXERCISE = '@242=3'

# Time sync parameters
TIME_PARAMS = {
    "@448": "month",
    "@449": "day",
    "@450": "year",
    "@402": "hour",
    "@403": "minute"
}

# Global state
_RUNNING = threading.Event()
_RUNNING.set()
logger = logging.getLogger('cummins_generator')


def remove_html_tags(text: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def get_local_time() -> datetime:
    """Get current local time in America/New_York timezone."""
    return datetime.now(TIMEZONE)


class Generator:
    """Cummins Generator controller and monitor."""
    
    # MQTT topic names (prefix will be set in __init__)
    MQTT_TOPICS = {
        'prefix': 'cummins/',  # Default, can be overridden
        'time': 'time',
        'date': 'date',
        'battery_voltage': 'battery_voltage',
        'load_1': 'load_1',
        'load_2': 'load_2',
        'output_volts': 'output_volts',
        'output_freq': 'output_freq',
        'engine_hrs': 'engine_hrs',
        'fault': 'fault',
        'fault_desc': 'fault_desc',
        'event': 'event',
        'event_desc': 'event_desc',
        'auto_mode': 'auto_mode',
        'utility_present': 'utility_present',
        'utility_connected': 'utility_connected',
        'running': 'running',
        'standby': 'standby',
        'action_required': 'action_required',
        'status': 'status',
        'exercising': 'exercising',
        'engine_exercise': 'engine_exercise',
        'engine_start': 'engine_start',
        'standby_enable': 'standby_enable',
        # New functionality topics
        'exercise_schedule': 'exercise_schedule',
        'event_log': 'event_log',
        'fault_log': 'fault_log',
        'load_control': 'load_control',
        'load_shed': 'load_shed',
    }

    def __init__(self, address: str, username: str, password: str, 
                 mqtt_prefix: str = "cummins/", 
                 device_name: str = "Cummins Generator",
                 unique_id: Optional[str] = None,
                 discovery_prefix: str = "homeassistant"):
        """Initialize generator connection."""
        self.address = address
        self.username = username
        self.password = password
        self.login_site = f"http://{username}:{password}@{address}/"
        self.data_page = "index_data.html"
        self.cgi_script = "wr_logical.cgi"
        self.exercise_page = "exercise.html"
        self.events_page = "events.html"
        self.faults_page = "faults.html"
        self.loads_page = "loads.html"
        self.loads_data_page = "loads_data.html"
        
        # Thread safety
        self.lock = threading.Lock()
        self.session = requests.Session()
        
        # MQTT configuration
        # Ensure prefix ends with / if not empty
        if mqtt_prefix and not mqtt_prefix.endswith('/'):
            mqtt_prefix = mqtt_prefix + '/'
        self.MQTT_TOPICS['prefix'] = mqtt_prefix
        
        # MQTT discovery configuration
        self.discovery_prefix = discovery_prefix
        self.unique_id = unique_id or f"cummins_generator_{address.replace('.', '_')}"
        self.device_name = device_name
        self.discovery_published = False
        
        # Initialize state tracking
        self._init_state()

    def _init_state(self):
        """Initialize all state variables."""
        # Data fields with their last values
        self.state = {
            'datetime': (None, None),
            'batt_volt': (None, None),
            'status': (None, None),
            'load_1': (None, None),
            'load_2': (None, None),
            'volts': (None, None),
            'freq': (None, None),
            'eng_hrs': (None, None),
            'fault': (None, None),
            'event': (None, None),
            'event_desc': (None, None),
            'fault_desc': (None, None),
            'auto_mode': (None, None),
            'utility_present': (None, None),
            'utility_connected': (None, None),
            'running': (None, None),
            'exercising': (None, None),
            'standby': (None, None),
            'action_required': (None, None),
        }

    def _get_state(self, key: str) -> Any:
        """Get current state value."""
        return self.state[key][0]

    def _get_last_state(self, key: str) -> Any:
        """Get last state value."""
        return self.state[key][1]

    def _set_state(self, key: str, value: Any):
        """Update state value, moving current to last."""
        current, _ = self.state[key]
        self.state[key] = (value, current)

    @contextmanager
    def _request_lock(self):
        """Context manager for thread-safe HTTP requests."""
        self.lock.acquire()
        try:
            yield
        finally:
            self.lock.release()

    def _make_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Make a thread-safe HTTP request."""
        kwargs.setdefault('timeout', REQUEST_TIMEOUT)
        with self._request_lock():
            try:
                if method.upper() == 'GET':
                    return self.session.get(url, **kwargs)
                elif method.upper() == 'POST':
                    return self.session.post(url, **kwargs)
            except requests.exceptions.Timeout as e:
                logger.warning(f"Request timeout connecting to generator: {e}")
                return None
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error connecting to generator: {e}")
                return None

    def _parse_status_data(self, status: list) -> Dict[str, Any]:
        """
        Parse raw status data into structured format.
        
        Data structure from index_data.html (matches JavaScript parse_vars):
        [0] = hour (0-23)
        [1] = minute (0-59)
        [2] = battery voltage (in tenths, e.g. 138 = 13.8V)
        [3] = status code (0=Stopped, 1=Stopped, 2-3=Starting, 4=Running, etc.)
        [4] = load line 1 (%)
        [5] = load line 2 (%)
        [6] = output voltage (VAC)
        [7] = frequency (Hz)
        [8] = engine hours (in minutes)
        [9] = month name (e.g. "January")
        [10] = day of month
        [11] = year
        [12] = LCD status (bit flags)
        [13] = current fault code
        [14] = current event code
        [15] = event description
        [16] = fault description
        [17] = auto mode
        """
        lcd_status = int(status[12])
        status_code = int(status[3])
        
        # Parse datetime: "January 14, 2026 1:18"
        dt_str = f"{status[9]} {status[10]}, {status[11]} {status[0]}:{status[1]}"
        parsed_datetime = datetime.strptime(dt_str, "%B %d, %Y %H:%M")
        
        return {
            'datetime': parsed_datetime,
            'batt_volt': float(status[2]) / 10,
            'load_1': int(status[4]),
            'load_2': int(status[5]),
            'volts': int(status[6]),
            'freq': int(status[7]),
            'eng_hrs': round(float(status[8]) / 60, 1),
            'fault': status[13],
            'event': status[14],
            'event_desc': status[15],
            'fault_desc': status[16],
            'auto_mode': status[17],
            'lcd_status': lcd_status,
            'status_code': status_code,
        }

    def _parse_lcd_status(self, lcd_status: int) -> Dict[str, bool]:
        """Parse LCD status bits into boolean flags."""
        return {
            'utility_present': bool(lcd_status & LCD_BIT_UTILITY_PRESENT),
            'utility_connected': bool(lcd_status & LCD_BIT_UTILITY_CONNECTED),
            'running': bool(lcd_status & LCD_BIT_RUNNING),
            'standby': not bool(lcd_status & LCD_BIT_STANDBY),
            'action_required': bool(lcd_status & LCD_BIT_ACTION_REQUIRED),
        }

    def _get_status_string(self, status_code: int, fault: str) -> str:
        """Convert status code to human-readable string."""
        if status_code in STATUS_CODES:
            status_str = STATUS_CODES[status_code]
            if status_code == 6:  # Fault status
                return f"{status_str} {fault}"
            return status_str
        return f"Unknown status - {status_code}"

    def get_data(self) -> bool:
        """Fetch and parse current generator status."""
        logger.debug('Fetching generator status data')
        
        response = self._make_request('GET', self.login_site + self.data_page)
        if not response or response.status_code != 200:
            if response:
                logger.warning(f'Failed to fetch generator data - HTTP {response.status_code}')
            else:
                logger.warning('Failed to fetch generator data - No response')
            return False

        # Parse HTML response
        status = remove_html_tags(response.text).split()
        if len(status) < 18:
            logger.warning(f"Unexpected status data length: {len(status)}, expected at least 18 fields")
            return False
        
        logger.debug(f"Successfully parsed generator status - {len(status)} fields")

        # Parse structured data
        data = self._parse_status_data(status)
        lcd_flags = self._parse_lcd_status(data['lcd_status'])
        
        # Update state
        self._set_state('datetime', data['datetime'])
        self._set_state('batt_volt', data['batt_volt'])
        self._set_state('load_1', data['load_1'])
        self._set_state('load_2', data['load_2'])
        self._set_state('volts', data['volts'])
        self._set_state('freq', data['freq'])
        self._set_state('eng_hrs', data['eng_hrs'])
        self._set_state('fault', data['fault'])
        self._set_state('event', data['event'])
        self._set_state('event_desc', data['event_desc'])
        self._set_state('fault_desc', data['fault_desc'])
        self._set_state('auto_mode', data['auto_mode'])
        
        # Update LCD flags
        for key, value in lcd_flags.items():
            self._set_state(key, value)
        
        # Calculate exercising state
        exercising = lcd_flags['running'] and lcd_flags['utility_connected']
        self._set_state('exercising', exercising)
        
        # Update status string
        status_str = self._get_status_string(data['status_code'], data['fault'])
        self._set_state('status', status_str)
        
        return True

    def set_time(self, dt: datetime) -> bool:
        """Set generator clock to specified datetime."""
        data = {
            "@448": dt.month,
            "@449": dt.day,
            "@450": dt.year,
            "@402": dt.hour,
            "@403": dt.minute
        }
        response = self._make_request('POST', self.login_site + self.cgi_script, data=data)
        return response is not None

    def check_time(self, delta_min: int = 0, sync: bool = False) -> int:
        """Check time difference and optionally sync if beyond threshold."""
        self.get_data()  # Always call get_data, even if it fails we use previous datetime
        
        current_time = get_local_time()
        gen_time = self._get_state('datetime')
        if gen_time is None:
            return 0
            
        gen_time_tz = gen_time.replace(tzinfo=TIMEZONE)
        difference = int((current_time - gen_time_tz).total_seconds() / 60)
        
        if abs(difference) >= delta_min and sync:
            self.sync_time()
        return difference

    def sync_time(self):
        """Synchronize generator clock with local time."""
        logger.info("Synchronizing generator time")
        result = self.set_time(get_local_time())
        if result:
            logger.info("Generator time synchronized successfully")
        else:
            logger.error("Failed to synchronize generator time")
        return result

    def push_button(self, parameters: str) -> bool:
        """Send button press command to generator."""
        url = f"{self.login_site}{self.cgi_script}?{parameters}"
        response = self._make_request('GET', url)
        return response is not None

    def standby_disable(self):
        """Disable standby mode."""
        logger.info("Sending standby disable command")
        result = self.push_button(PARAM_STANDBY_DISABLE)
        if result:
            logger.info("Standby disable command sent successfully")
        else:
            logger.error("Failed to send standby disable command")
        return result

    def standby_enable(self):
        """Enable standby mode."""
        logger.info("Sending standby enable command")
        result = self.push_button(PARAM_STANDBY_ENABLE)
        if result:
            logger.info("Standby enable command sent successfully")
        else:
            logger.error("Failed to send standby enable command")
        return result

    def engine_start(self):
        """Start the generator engine."""
        logger.info("Sending engine start command")
        result = self.push_button(PARAM_ENGINE_START)
        if result:
            logger.info("Engine start command sent successfully")
        else:
            logger.error("Failed to send engine start command")
        return result

    def engine_stop(self):
        """Stop the generator engine."""
        logger.info("Sending engine stop command")
        result = self.push_button(PARAM_ENGINE_STOP)
        if result:
            logger.info("Engine stop command sent successfully")
        else:
            logger.error("Failed to send engine stop command")
        return result

    def engine_exercise(self):
        """Start engine exercise cycle."""
        logger.info("Sending engine exercise command")
        result = self.push_button(PARAM_ENGINE_EXERCISE)
        if result:
            logger.info("Engine exercise command sent successfully")
        else:
            logger.error("Failed to send engine exercise command")
        return result

    def get_exercise_schedule(self) -> Optional[Dict[str, Any]]:
        """Get exercise schedule configuration."""
        response = self._make_request('GET', self.login_site + self.exercise_page)
        if not response or response.status_code != 200:
            return None
        
        try:
            import re
            page_data_match = re.search(r'var\s+page_data\s*=\s*["\']([^"\']+)["\']', response.text, re.DOTALL)
            schedule_data = {}
            
            if page_data_match:
                data = page_data_match.group(1)
                lines = [l for l in data.split('\\n') if l.strip()]
                if len(lines) >= 7:
                    schedule_data['raw_data'] = lines
            
            form_matches = re.findall(r'name=["\'](\w+)["\'].*?value=["\']?(\w+)["\']?', response.text)
            for name, value in form_matches:
                if 'day' in name.lower() or 'time' in name.lower() or 'enable' in name.lower():
                    schedule_data[name] = value
            
            return schedule_data if schedule_data else None
        except Exception as e:
            logger.debug(f"Error parsing exercise schedule: {e}")
            return None

    def get_event_log(self, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
        """Get event log history."""
        response = self._make_request('GET', self.login_site + self.events_page)
        if not response or response.status_code != 200:
            return None
        
        try:
            import re
            events = []
            
            page_data_match = re.search(r'var\s+page_data\s*=\s*["\']([^"\']+)["\']', response.text, re.DOTALL)
            if page_data_match:
                data = page_data_match.group(1)
                lines = [l for l in data.split('\\n') if l.strip()]
                
                for i in range(0, min(len(lines), limit * 5), 5):
                    if i + 4 < len(lines):
                        event = {
                            'index': i // 5,
                            'code': lines[i] if i < len(lines) else '',
                            'description': lines[i+1] if i+1 < len(lines) else '',
                            'timestamp': lines[i+2] if i+2 < len(lines) else '',
                        }
                        events.append(event)
            
            if BeautifulSoup:
                soup = BeautifulSoup(response.text, 'html.parser')
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows[1:]:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 3:
                            event = {
                                'code': cells[0].get_text(strip=True),
                                'description': cells[1].get_text(strip=True),
                                'timestamp': cells[2].get_text(strip=True) if len(cells) > 2 else '',
                            }
                            if event['code']:
                                events.append(event)
            
            return events[:limit] if events else None
        except Exception as e:
            logger.debug(f"Error parsing event log: {e}")
            return None

    def get_fault_log(self, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
        """Get fault log history."""
        response = self._make_request('GET', self.login_site + self.faults_page)
        if not response or response.status_code != 200:
            return None
        
        try:
            import re
            faults = []
            
            page_data_match = re.search(r'var\s+page_data\s*=\s*["\']([^"\']+)["\']', response.text, re.DOTALL)
            if page_data_match:
                data = page_data_match.group(1)
                lines = [l for l in data.split('\\n') if l.strip()]
                
                for i in range(0, min(len(lines), limit * 5), 5):
                    if i + 4 < len(lines):
                        fault = {
                            'index': i // 5,
                            'code': lines[i] if i < len(lines) else '',
                            'description': lines[i+1] if i+1 < len(lines) else '',
                            'timestamp': lines[i+2] if i+2 < len(lines) else '',
                        }
                        faults.append(fault)
            
            if BeautifulSoup:
                soup = BeautifulSoup(response.text, 'html.parser')
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows[1:]:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 3:
                            fault = {
                                'code': cells[0].get_text(strip=True),
                                'description': cells[1].get_text(strip=True),
                                'timestamp': cells[2].get_text(strip=True) if len(cells) > 2 else '',
                            }
                            if fault['code']:
                                faults.append(fault)
            
            return faults[:limit] if faults else None
        except Exception as e:
            logger.debug(f"Error parsing fault log: {e}")
            return None

    def get_load_control(self) -> Optional[Dict[str, Any]]:
        """Get load control status."""
        response = self._make_request('GET', self.login_site + self.loads_data_page)
        if not response or response.status_code != 200:
            return None
        
        try:
            data = remove_html_tags(response.text).strip()
            values = [v for v in data.split() if v]
            
            load_control = {}
            if len(values) >= 2:
                load_control['load_shed_1'] = int(values[0]) if values[0].isdigit() else 0
                load_control['load_shed_2'] = int(values[1]) if len(values) > 1 and values[1].isdigit() else 0
            
            return load_control
        except Exception as e:
            logger.debug(f"Error parsing load control: {e}")
            return None

    def set_load_shed(self, line: int, enabled: bool):
        """Set load shedding for a line (1 or 2)."""
        # Load shedding parameters need to be determined from loads.html
        # This is a placeholder - actual parameters need to be discovered
        logger.debug(f"Load shed control - line {line}, enabled {enabled}")
        # TODO: Determine correct CGI parameters for load shedding from loads.html

    def _publish_if_changed(self, mqtt_client: mqtt.Client, topic: str, current: Any, last: Any):
        """Publish MQTT message only if value has changed."""
        if current != last:
            full_topic = f"{self.MQTT_TOPICS['prefix']}{topic}"
            # Convert boolean to string for MQTT
            payload = str(current) if isinstance(current, bool) else current
            mqtt_client.publish(full_topic, payload, MQTT_QOS, MQTT_RETAIN)

    def publish_mqtt(self, mqtt_client: mqtt.Client):
        """Publish all changed generator status to MQTT."""
        # Check if MQTT client is connected before publishing
        if not mqtt_client.is_connected():
            logger.debug('Skipping MQTT publish - client not connected')
            return
            
        logger.debug('Publishing generator status to MQTT')
        if not self.get_data():
            logger.warning('Skipping MQTT publish - failed to get generator data')
            return

        # Publish datetime (time and date together)
        datetime_current = self._get_state('datetime')
        datetime_last = self._get_last_state('datetime')
        if datetime_current != datetime_last:
            prefix = self.MQTT_TOPICS['prefix']
            mqtt_client.publish(f"{prefix}{self.MQTT_TOPICS['time']}", 
                              datetime_current.strftime("%H:%M"), MQTT_QOS, MQTT_RETAIN)
            mqtt_client.publish(f"{prefix}{self.MQTT_TOPICS['date']}", 
                              datetime_current.strftime("%m/%d/%Y"), MQTT_QOS, MQTT_RETAIN)

        # Publish all other state changes
        state_mappings = [
            ('batt_volt', 'battery_voltage'),
            ('load_1', 'load_1'),
            ('load_2', 'load_2'),
            ('volts', 'output_volts'),
            ('freq', 'output_freq'),
            ('eng_hrs', 'engine_hrs'),
            ('fault', 'fault'),
            ('fault_desc', 'fault_desc'),
            ('event', 'event'),
            ('event_desc', 'event_desc'),
            ('auto_mode', 'auto_mode'),
            ('utility_present', 'utility_present'),
            ('utility_connected', 'utility_connected'),
            ('running', 'running'),
            ('exercising', 'exercising'),
            ('standby', 'standby'),
            ('action_required', 'action_required'),
            ('status', 'status'),
        ]
        
        for state_key, topic_key in state_mappings:
            current = self._get_state(state_key)
            last = self._get_last_state(state_key)
            self._publish_if_changed(mqtt_client, self.MQTT_TOPICS[topic_key], current, last)

    def publish_logs_mqtt(self, mqtt_client: mqtt.Client, include_schedule: bool = False):
        """Publish event log, fault log, and optionally exercise schedule to MQTT."""
        prefix = self.MQTT_TOPICS['prefix']
        published_count = 0
        
        # Publish event log
        event_log = self.get_event_log(limit=10)
        if event_log:
            payload = json.dumps(event_log)
            mqtt_client.publish(f"{prefix}{self.MQTT_TOPICS['event_log']}", payload, MQTT_QOS, MQTT_RETAIN)
            published_count += 1
            logger.debug(f"Published event log - {len(event_log)} events")
        else:
            logger.debug("No event log data to publish")
        
        # Publish fault log
        fault_log = self.get_fault_log(limit=10)
        if fault_log:
            payload = json.dumps(fault_log)
            mqtt_client.publish(f"{prefix}{self.MQTT_TOPICS['fault_log']}", payload, MQTT_QOS, MQTT_RETAIN)
            published_count += 1
            logger.debug(f"Published fault log - {len(fault_log)} faults")
        else:
            logger.debug("No fault log data to publish")
        
        # Publish load control status
        load_control = self.get_load_control()
        if load_control:
            payload = json.dumps(load_control)
            mqtt_client.publish(f"{prefix}{self.MQTT_TOPICS['load_control']}", payload, MQTT_QOS, MQTT_RETAIN)
            published_count += 1
            logger.debug(f"Published load control status")
        else:
            logger.debug("No load control data to publish")
        
        # Optionally publish exercise schedule
        if include_schedule:
            schedule = self.get_exercise_schedule()
            if schedule:
                payload = json.dumps(schedule)
                mqtt_client.publish(f"{prefix}{self.MQTT_TOPICS['exercise_schedule']}", payload, MQTT_QOS, MQTT_RETAIN)
                published_count += 1
                logger.debug("Published exercise schedule")
            else:
                logger.debug("No exercise schedule data to publish")
        
        logger.info(f"Published {published_count} log/schedule items to MQTT")

    def _get_device_info(self) -> Dict[str, Any]:
        """Get device information for MQTT discovery."""
        return {
            "identifiers": [self.unique_id],
            "name": self.device_name,
            "manufacturer": "Cummins",
            "model": "Standby Generator",
            "sw_version": __version__,
            "via_device": None,
        }

    def _publish_discovery_config(self, mqtt_client: mqtt.Client):
        """Publish MQTT autodiscovery configuration for Home Assistant."""
        logger.info(f"Starting autodiscovery config publication (discovery_prefix: {self.discovery_prefix})")
        prefix = self.MQTT_TOPICS['prefix']
        device_info = self._get_device_info()
        
        # Sensors
        sensors = [
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_battery_voltage",
                "name": "Battery Voltage",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['battery_voltage']}",
                "device_class": "voltage",
                "unit_of_measurement": "V",
                "state_class": "measurement",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_load_1",
                "name": "Load Line 1",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['load_1']}",
                "unit_of_measurement": "%",
                "state_class": "measurement",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_load_2",
                "name": "Load Line 2",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['load_2']}",
                "unit_of_measurement": "%",
                "state_class": "measurement",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_output_voltage",
                "name": "Output Voltage",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['output_volts']}",
                "device_class": "voltage",
                "unit_of_measurement": "V",
                "state_class": "measurement",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_output_frequency",
                "name": "Output Frequency",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['output_freq']}",
                "device_class": "frequency",
                "unit_of_measurement": "Hz",
                "state_class": "measurement",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_engine_hours",
                "name": "Engine Hours",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['engine_hrs']}",
                "unit_of_measurement": "h",
                "state_class": "total_increasing",
                "icon": "mdi:clock-outline",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_status",
                "name": "Status",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['status']}",
                "icon": "mdi:information",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_fault",
                "name": "Fault Code",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['fault']}",
                "icon": "mdi:alert",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_fault_desc",
                "name": "Fault Description",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['fault_desc']}",
                "icon": "mdi:alert-circle",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_event",
                "name": "Event Code",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['event']}",
                "icon": "mdi:bell",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_event_desc",
                "name": "Event Description",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['event_desc']}",
                "icon": "mdi:bell-ring",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_auto_mode",
                "name": "Auto Mode",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['auto_mode']}",
                "icon": "mdi:auto-mode",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_time",
                "name": "Generator Time",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['time']}",
                "icon": "mdi:clock",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_date",
                "name": "Generator Date",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['date']}",
                "icon": "mdi:calendar",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_event_log",
                "name": "Event Log",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['event_log']}",
                "icon": "mdi:history",
                "value_template": "{{ value_json | length }} events",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_fault_log",
                "name": "Fault Log",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['fault_log']}",
                "icon": "mdi:alert-octagon",
                "value_template": "{{ value_json | length }} faults",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_exercise_schedule",
                "name": "Exercise Schedule",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['exercise_schedule']}",
                "icon": "mdi:calendar-clock",
            },
            {
                "component": "sensor",
                "unique_id": f"{self.unique_id}_load_control",
                "name": "Load Control",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['load_control']}",
                "icon": "mdi:lightning-bolt",
            },
        ]
        
        # Binary sensors
        binary_sensors = [
            {
                "component": "binary_sensor",
                "unique_id": f"{self.unique_id}_utility_present",
                "name": "Utility Present",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['utility_present']}",
                "device_class": "power",
                "value_template": "{{ 'ON' if value|upper == 'TRUE' else 'OFF' }}",
            },
            {
                "component": "binary_sensor",
                "unique_id": f"{self.unique_id}_utility_connected",
                "name": "Utility Connected",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['utility_connected']}",
                "device_class": "connectivity",
                "value_template": "{{ 'ON' if value|upper == 'TRUE' else 'OFF' }}",
            },
            {
                "component": "binary_sensor",
                "unique_id": f"{self.unique_id}_running",
                "name": "Running",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['running']}",
                "device_class": "running",
                "value_template": "{{ 'ON' if value|upper == 'TRUE' else 'OFF' }}",
            },
            {
                "component": "binary_sensor",
                "unique_id": f"{self.unique_id}_standby",
                "name": "Standby",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['standby']}",
                "device_class": "power",
                "value_template": "{{ 'ON' if value|upper == 'TRUE' else 'OFF' }}",
            },
            {
                "component": "binary_sensor",
                "unique_id": f"{self.unique_id}_action_required",
                "name": "Action Required",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['action_required']}",
                "device_class": "problem",
                "value_template": "{{ 'ON' if value|upper == 'TRUE' else 'OFF' }}",
            },
            {
                "component": "binary_sensor",
                "unique_id": f"{self.unique_id}_exercising",
                "name": "Exercising",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['exercising']}",
                "value_template": "{{ 'ON' if value|upper == 'TRUE' else 'OFF' }}",
                "icon": "mdi:run",
            },
        ]
        
        # Buttons
        buttons = [
            {
                "component": "button",
                "unique_id": f"{self.unique_id}_engine_start",
                "name": "Engine Start",
                "command_topic": f"{prefix}{self.MQTT_TOPICS['engine_start']}",
                "payload_press": "True",
                "icon": "mdi:engine",
            },
            {
                "component": "button",
                "unique_id": f"{self.unique_id}_engine_stop",
                "name": "Engine Stop",
                "command_topic": f"{prefix}{self.MQTT_TOPICS['engine_start']}",
                "payload_press": "False",
                "icon": "mdi:engine-off",
            },
            {
                "component": "button",
                "unique_id": f"{self.unique_id}_engine_exercise",
                "name": "Engine Exercise",
                "command_topic": f"{prefix}{self.MQTT_TOPICS['engine_exercise']}",
                "payload_press": "True",
                "icon": "mdi:run",
            },
        ]
        
        # Switches
        switches = [
            {
                "component": "switch",
                "unique_id": f"{self.unique_id}_standby_enable",
                "name": "Standby Enable",
                "state_topic": f"{prefix}{self.MQTT_TOPICS['standby']}",
                "command_topic": f"{prefix}{self.MQTT_TOPICS['standby_enable']}",
                "state_on": "True",
                "state_off": "False",
                "payload_on": "True",
                "payload_off": "False",
                "value_template": "{{ 'True' if value == 'True' else 'False' }}",
                "icon": "mdi:power",
            },
        ]
        
        # Publish all discovery configs
        all_entities = sensors + binary_sensors + buttons + switches
        
        for entity in all_entities:
            config_topic = f"{self.discovery_prefix}/{entity['component']}/{entity['unique_id']}/config"
            
            config = {
                "name": entity["name"],
                "unique_id": entity["unique_id"],
                "device": device_info,
                "availability_topic": f"{prefix}status",
                "payload_available": "online",
                "payload_not_available": "offline",
            }
            
            # Add component-specific config
            if entity["component"] in ["sensor", "binary_sensor"]:
                config["state_topic"] = entity["state_topic"]
                if "device_class" in entity:
                    config["device_class"] = entity["device_class"]
                if "unit_of_measurement" in entity:
                    config["unit_of_measurement"] = entity["unit_of_measurement"]
                if "state_class" in entity:
                    config["state_class"] = entity["state_class"]
                if "value_template" in entity:
                    config["value_template"] = entity["value_template"]
                if "icon" in entity:
                    config["icon"] = entity["icon"]
            elif entity["component"] == "button":
                config["command_topic"] = entity["command_topic"]
                config["payload_press"] = entity["payload_press"]
                if "icon" in entity:
                    config["icon"] = entity["icon"]
            elif entity["component"] == "switch":
                config["state_topic"] = entity["state_topic"]
                config["command_topic"] = entity["command_topic"]
                config["state_on"] = entity["state_on"]
                config["state_off"] = entity["state_off"]
                config["payload_on"] = entity["payload_on"]
                config["payload_off"] = entity["payload_off"]
                if "value_template" in entity:
                    config["value_template"] = entity["value_template"]
                if "icon" in entity:
                    config["icon"] = entity["icon"]
            
            payload = json.dumps(config)
            try:
                result = mqtt_client.publish(config_topic, payload, MQTT_QOS, True)
                if result.rc == 0:  # MQTT_ERR_SUCCESS
                    logger.debug(f"Published discovery config: {config_topic}")
                else:
                    logger.warning(f"Failed to publish discovery config {config_topic}: return code {result.rc}")
            except Exception as e:
                logger.error(f"Error publishing discovery config {config_topic}: {e}")
        
        self.discovery_published = True
        logger.info(f"MQTT autodiscovery configuration published - {len(all_entities)} entities to {self.discovery_prefix}/")

    def subscribe_mqtt(self, mqtt_client: mqtt.Client):
        """Subscribe to MQTT control topics."""
        prefix = self.MQTT_TOPICS['prefix']
        
        subscriptions = [
            (self.MQTT_TOPICS['engine_exercise'], self.on_message_engine_exercise),
            (self.MQTT_TOPICS['standby_enable'], self.on_message_standby_enable),
            (self.MQTT_TOPICS['engine_start'], self.on_message_engine_start),
        ]
        
        for topic, callback in subscriptions:
            full_topic = f"{prefix}{topic}"
            mqtt_client.message_callback_add(full_topic, callback)
            mqtt_client.subscribe(full_topic)
        
        # Publish autodiscovery config on first connection
        if not self.discovery_published:
            self._publish_discovery_config(mqtt_client)

    def _decode_payload(self, msg) -> str:
        """Decode MQTT message payload."""
        if isinstance(msg.payload, bytes):
            return msg.payload.decode("utf-8")
        return str(msg.payload)

    def on_message_engine_exercise(self, mosq, obj, msg):
        """Handle engine exercise MQTT command."""
        payload = self._decode_payload(msg)
        if payload == "True":
            self.engine_exercise()
        elif payload == "False":
            self.engine_stop()

    def on_message_standby_enable(self, mosq, obj, msg):
        """Handle standby enable MQTT command."""
        payload = self._decode_payload(msg)
        if payload == "True":
            self.standby_enable()
        elif payload == "False":
            self.standby_disable()
        # Switch state is automatically updated via the standby state topic

    def on_message_engine_start(self, mosq, obj, msg):
        """Handle engine start MQTT command."""
        payload = self._decode_payload(msg)
        if payload == "True":
            self.engine_start()
        elif payload == "False":
            self.engine_stop()


def time_sync(generator: Generator, time_delay: int, running_flag: threading.Event):
    """Sync generator time periodically in background thread."""
    logger.info(f"Time sync thread started - interval: {time_delay} seconds")
    time_start = time.time()
    while running_flag.is_set():
        time_now = time.time()
        time_diff = time_now - time_start
        if time_diff >= time_delay:
            try:
                difference = generator.check_time(5, True)
                if abs(difference) > 0:
                    logger.info(f"Generator time synchronized - difference: {difference} minutes")
                else:
                    logger.debug(f"Generator time check - difference: {difference} minutes")
            except Exception as e:
                logger.error(f"Error during time sync: {e}")
            time_start = time.time()
        time.sleep(1)
    logger.info("Time sync thread stopped")


class ServiceExit(Exception):
    """Custom exception to trigger clean exit of all running threads."""
    pass
 
 
def service_shutdown(signum, frame):
    """Handle shutdown signals."""
    logger.debug(f'Caught signal {signum}')
    _RUNNING.clear()
    raise ServiceExit


def setup_logging(debug: bool = False, log_level: Optional[str] = None):
    """
    Configure logging with console output only.
    
    Args:
        debug: Enable debug mode (sets level to DEBUG)
        log_level: Log level as string (DEBUG, INFO, WARNING, ERROR) - overrides debug
    """
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Determine log level
    if log_level:
        level = getattr(logging, log_level.upper(), logging.INFO)
    elif debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)-8s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized - Level: {logging.getLevelName(level)}")
    if debug:
        logger.debug("Debug logging enabled")


def load_config(config_path: str) -> configparser.ConfigParser:
    """Load and return configuration."""
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def create_mqtt_client(config: configparser.ConfigParser) -> mqtt.Client:
    """Create and configure MQTT client."""
    # Use callback API version 1 for compatibility with existing callback signatures
    try:
        # paho-mqtt 2.0+ requires explicit callback API version as first positional argument
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="cummins")
    except (AttributeError, TypeError):
        # Fallback for older versions of paho-mqtt that don't have CallbackAPIVersion
        mqtt_client = mqtt.Client("cummins")
    
    mqtt_host = config.get('MQTT', 'Host', fallback='localhost')
    mqtt_port = config.getint('MQTT', 'Port', fallback=1883)
    mqtt_username = config.get('MQTT', 'Username', fallback=None)
    mqtt_password = config.get('MQTT', 'Password', fallback=None)
    
    logger.info(f"Connecting to MQTT broker at {mqtt_host}:{mqtt_port}")
    if mqtt_username:
        logger.debug(f"Using MQTT username: {mqtt_username}")
    else:
        logger.debug("No MQTT username configured (anonymous connection)")
    
    if mqtt_username and mqtt_password:
        mqtt_client.username_pw_set(mqtt_username, mqtt_password)
    
    try:
        # Note: connect() is non-blocking when using loop_start() later
        # It will establish the connection asynchronously
        result = mqtt_client.connect(mqtt_host, mqtt_port, 60)
        if result != 0:  # 0 = MQTT_ERR_SUCCESS
            logger.warning(f"MQTT connect() returned non-zero: {result} (connection will be established asynchronously)")
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {e}")
        raise
    
    return mqtt_client


def main():
    """Main application entry point."""
    try:
        # Register signal handlers
        signal.signal(signal.SIGTERM, service_shutdown)
        signal.signal(signal.SIGINT, service_shutdown)

        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('-c', '--config', default='./config.ini', 
                          help='The path to the config file')
        parser.add_argument('-d', '--debug', action='store_true', default=False, 
                          help='Enable debug logging')
        parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                          default=None, help='Set logging level (overrides --debug)')
        args = parser.parse_args()
        
        # Load configuration
        config = load_config(args.config)
        
        # Get logging settings from config if not provided via command line
        log_level = args.log_level or config.get('LOGGING', 'LogLevel', fallback=None)
        
        # Setup logging (must be done after config is loaded)
        setup_logging(debug=args.debug, log_level=log_level)
        
        # Initialize generator
        logger.info('Initializing generator connection')
        mqtt_prefix = config.get('MQTT', 'Prefix', fallback='cummins/')
        device_name = config.get('MQTT', 'DeviceName', fallback='Cummins Generator')
        unique_id = config.get('MQTT', 'UniqueID', fallback=None)
        discovery_prefix = config.get('MQTT', 'DiscoveryPrefix', fallback='homeassistant')
        
        # Use None for unique_id if empty string
        if unique_id == '':
            unique_id = None
        
        cummins = Generator(
            config['CUMMINS']['Host'],
            config['CUMMINS']['Username'],
            config['CUMMINS']['Password'],
            mqtt_prefix=mqtt_prefix,
            device_name=device_name,
            unique_id=unique_id,
            discovery_prefix=discovery_prefix
        )
        
        # Setup MQTT
        mqtt_client = create_mqtt_client(config)
        
        # Set up on_connect callback to publish discovery config
        def on_connect(client, userdata, flags, rc):
            # MQTT return codes: 0=success, 1-5=errors
            mqtt_rc_codes = {
                0: "Connection successful",
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorised"
            }
            
            if rc == 0:
                logger.info(f"MQTT broker connected successfully - {mqtt_rc_codes.get(rc, 'Unknown')}")
                # Publish availability status
                prefix = cummins.MQTT_TOPICS['prefix']
                result = client.publish(f"{prefix}status", "online", MQTT_QOS, True)
                if result.rc == 0:
                    logger.debug("Published MQTT availability: online")
                else:
                    logger.warning(f"Failed to publish availability status: return code {result.rc}")
                # Subscribe to control topics and publish discovery
                cummins.subscribe_mqtt(client)
            else:
                error_msg = mqtt_rc_codes.get(rc, f"Unknown error code {rc}")
                logger.error(f"MQTT connection failed: {error_msg} (return code {rc})")
                if rc == 4:
                    logger.error("Check MQTT username and password in configuration")
                elif rc == 5:
                    logger.error("MQTT broker rejected connection - check broker ACL/permissions")
        
        def on_disconnect(client, userdata, rc):
            if rc == 0:
                logger.info("MQTT broker disconnected normally")
            else:
                logger.warning(f"MQTT broker disconnected unexpectedly (rc={rc})")
            prefix = cummins.MQTT_TOPICS['prefix']
            client.publish(f"{prefix}status", "offline", MQTT_QOS, True)
            logger.debug("Published MQTT availability: offline")
        
        mqtt_client.on_connect = on_connect
        mqtt_client.on_disconnect = on_disconnect
        
        mqtt_client.loop_start()
        
        # Wait a moment for connection to establish, then check status
        time.sleep(2)
        if mqtt_client.is_connected():
            logger.info("MQTT client is connected and ready")
        else:
            logger.warning("MQTT client is not connected - will retry in background")

        # Start time sync thread
        logger.info('Starting time synchronization thread')
        time_sync_interval = int(config['CUMMINS']['TimeSyncMin']) * 60
        time_thread = threading.Thread(
            target=time_sync,
            args=(cummins, time_sync_interval, _RUNNING)
        )
        time_thread.daemon = True
        time_thread.start()

        # Main loop
        try:
            # Track last time logs were published (publish every 5 minutes)
            last_log_publish = time.time()
            log_publish_interval = 300  # 5 minutes
            
            # Track autodiscovery publishing - try to publish after a short delay
            autodiscovery_published = False
            autodiscovery_retry_time = time.time() + 5  # Try after 5 seconds

            while _RUNNING.is_set():
                time.sleep(1)
                
                # Try to publish autodiscovery if not yet published and enough time has passed
                if not autodiscovery_published and time.time() >= autodiscovery_retry_time:
                    if mqtt_client.is_connected():
                        logger.info("Publishing autodiscovery configuration (retry)")
                        cummins.subscribe_mqtt(mqtt_client)
                        autodiscovery_published = cummins.discovery_published
                    else:
                        # Retry again in 5 seconds if not connected
                        autodiscovery_retry_time = time.time() + 5
                
                cummins.publish_mqtt(mqtt_client)
                
                # Periodically publish logs and schedule
                current_time = time.time()
                if current_time - last_log_publish >= log_publish_interval:
                    logger.info('Publishing logs and schedule data (periodic update)')
                    cummins.publish_logs_mqtt(mqtt_client, include_schedule=True)
                    last_log_publish = current_time

        except ServiceExit:
            _RUNNING.clear()
            time_thread.join(timeout=5)
        finally:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            logger.info('Shutting down - exiting main program')
            
    except KeyError as e:
        logger.error(f"Missing required configuration: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
