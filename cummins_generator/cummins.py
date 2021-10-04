from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import argparse
import multiprocessing
import threading
import configparser
import pytz
from tzlocal import get_localzone
import signal
import logging
import sys


_RUNNING = True
logger = logging.getLogger()


def remove_html_tags(text):
    """Remove html tags from a string"""
    import re
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


class Generator:
    def __init__(self, address, username, password):
        self.address = address
        self.username = username
        self.password = password
        self.login_site = "http://{}:{}@{}/".format(self.username, self.password, self.address)
        self.data_page = "index_data.html"
        self.cgi_script = "wr_logical.cgi"
        self.mqtt_prefix = "cummins/"
        self.mqtt_time = "time"
        self.mqtt_date = "date"
        self.mqtt_battery_voltage = "battery_voltage"
        self.mqtt_load_1 = "load_1"
        self.mqtt_load_2 = "load_2"
        self.mqtt_output_volts = "output_volts"
        self.mqtt_output_freq = "output_freq"
        self.mqtt_engine_hrs = "engine_hrs"
        self.mqtt_fault = "fault"
        self.mqtt_fault_desc = "fault_desc"
        self.mqtt_event = "event"
        self.mqtt_event_desc = "event_desc"
        self.mqtt_auto_mode = "auto_mode"
        self.mqtt_utility_present = "utility_present"
        self.mqtt_utility_connected = "utility_connected"
        self.mqtt_running = "running"
        self.mqtt_standby = "standby"
        self.mqtt_action_required = "action_required"
        self.mqtt_status = "status"
        self.mqtt_exercising = "exercising"
        self.mqtt_engine_exercise = "engine_exercise"
        self.mqtt_engine_start = "engine_start"
        self.mqtt_engine_stop = "engine_stop"

        self.mqtt_standby_enable = "standby_enable"


        self.datetime = None
        self.last_datetime = None

        self.batt_volt = None
        self.last_batt_volt = None

        self.status = None
        self.last_status = None

        self.load_1 = None
        self.last_load_1 = None

        self.load_2 = None
        self.last_load_2 = None

        self.volts = None
        self.last_volts = None

        self.freq = None
        self.last_freq = None

        self.eng_hrs = None
        self.last_eng_hrs = None


        self.lcd_status = None
        self.last_lcd_status = None

        self.fault = None
        self.last_fault = None

        self.event = None
        self.last_event = None

        self.event_desc = None
        self.last_event_desc = None

        self.fault_desc = None
        self.last_fault_desc = None

        self.auto_mode = None
        self.last_auto_mode = None

        self.utility_present = None
        self.last_utility_present = None

        self.utility_connected = None
        self.last_utility_connected = None

        self.running = None
        self.last_running = None

        self.exercising = None
        self.last_exercising = None

        self.standby = None
        self.last_standby = None

        self.action_required = None
        self.last_action_required = None



        self.lock = threading.Lock()

        # Start the session
        self.session = requests.Session()


    def get_data(self):

        # Navigate to the next page and scrape the data
        logger.debug('Reaching out to Generator for status update')
        self.lock.acquire()
        try:
            s = self.session.get(self.login_site+self.data_page)
        except:
            logger.debug("Failed to connect to Generator")
            self.lock.release()
            return False
        self.lock.release()

        if s.status_code != 200:
            logger.debug('HTTP', s.status_code)
            return False
        else:

            status = remove_html_tags(s.text).split()

            dt = "{} {}, {} {}:{}".format(status[9], status[10], status[11], status[0], status[1])
            self.last_datetime = self.datetime
            self.datetime = datetime.strptime(dt, "%B %d, %Y %H:%M")

            self.last_batt_volt = self.batt_volt
            self.batt_volt = float(status[2])/10

            self.last_load_1 = self.load_1
            self.load_1 = int(status[4])

            self.last_load_2 = self.load_2
            self.load_2 = int(status[5])

            self.last_volts = self.volts
            self.volts = int(status[6])

            self.last_freq = self.freq
            self.freq = int(status[7])

            self.last_eng_hrs = self.eng_hrs
            self.eng_hrs = round(float(status[8])/60, 1)

            self.last_fault = self.fault
            self.fault = status[13]

            self.last_event = self.event
            self.event = status[14]

            self.last_event_desc = self.event_desc
            self.event_desc = status[15]

            self.last_fault_desc = self.fault_desc
            self.fault_desc = status[16]

            self.last_auto_mode = self.auto_mode
            self.auto_mode = status[17]

            lcd_status = int(status[12])

            self.last_utility_present = self.utility_present
            if lcd_status & 0x01:
                self.utility_present = True
            else:
                self.utility_present = False

            self.last_utility_connected = self.utility_connected
            if lcd_status & 0x02:
                self.utility_connected = True
            else:
                self.utility_connected = False

            self.last_running = self.running
            if lcd_status & 0x0C:
                self.running = True
            else:
                self.running = False

            self.last_standby = self.standby
            if lcd_status & 0x10:
                self.standby = False
            else:
                self.standby = True

            self.last_action_required = self.action_required
            if lcd_status & 0x60:
                self.action_required = True
            else:
                self.action_required = False

            self.last_exercising = self.exercising
            if self.running and self.utility_connected:
                self.exercising = True
            else:
                self.exercising = False

            status_code = int(status[3])

            self.last_status = self.status
            if status_code == 0 or status_code == 1:
                self.status = "Stopped"
            elif status_code == 2:
                self.status = "Starting"
            elif status_code == 3:
                self.status = "Starting"
            elif status_code == 4:
                self.status = "Running"
            elif status_code == 5:
                self.status = "Priming"
            elif status_code == 6:
                self.status = "Fault {}".format(self.fault)
            elif status_code == 7:
                self.status = "Eng.Only"
            elif status_code == 8:
                self.status = "TestMode"
            elif status_code == 9:
                self.status = "Volt Adj"
            elif status_code == 20:
                self.status = "** Config Mode **"
            elif status_code == 21:
                self.status = "Cycle crank pause"
            elif status_code == 22:
                self.status = "Exercising"
            elif status_code == 23:
                self.status = "Engine Cooldown"
            else:
                self.status = "Unknown status - {}".format(status[1])
        return True

    def set_time(self, dt):
        data = {
            "@448": dt.month,
            "@449": dt.day,
            "@450": dt.year,
            "@402": dt.hour,
            "@403": dt.minute
        }
        self.lock.acquire()
        try:
            results = self.session.post(self.login_site + self.cgi_script, data)
        except:
            self.lock.release()
            logger.debug("Failed to connect to Generator")
            return False
        self.lock.release()
        return True

    def check_time(self, delta_min=0, sync=False):
        self.get_data()
        difference = int((get_local_time() - self.datetime.replace( tzinfo=pytz.timezone('America/New_York'))).total_seconds() / 60)
        if abs(difference) >= delta_min and sync == True:
            self.sync_time()
        return difference

    def sync_time(self):
        self.set_time(get_local_time())

    def push_button(self, parameters):
        self.lock.acquire()
        try:
            results = self.session.get(self.login_site + self.cgi_script + "?" + parameters)
        except:
            logger.debug("Failed to connect to Generator")
            self.lock.release()
            return False
        self.lock.release()
        return True

    def standby_disable(self):
        self.push_button('@385=0')

    def standby_enable(self):
        self.push_button('@385=1')

    def engine_start(self):
        self.push_button('@242=2')

    def engine_stop(self):
        self.push_button('@242=1')

    def engine_exercise(self):
        self.push_button('@242=3')

    def publish_mqtt(self,mqtt_client):
        logger.debug('Getting Data')

        self.get_data()

        logger.debug('Publishing MQTT Messages')

        if self.datetime != self.last_datetime:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_time, self.datetime.strftime("%H:%M"), 0, True)
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_date, self.datetime.strftime("%m/%d/%Y"), 0, True)
        if self.batt_volt != self.last_batt_volt:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_battery_voltage, self.batt_volt, 0, True)
        if self.load_1 != self.last_load_1:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_load_1, self.load_1, 0, True)
        if self.load_2 != self.last_load_2:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_load_2, self.load_2, 0, True)
        if self.volts != self.last_volts:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_output_volts, self.volts, 0, True)
        if self.freq != self.last_freq:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_output_freq, self.freq, 0, True)
        if self.eng_hrs != self.last_eng_hrs:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_engine_hrs, self.eng_hrs, 0, True)
        if self.fault != self.last_fault:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_fault, self.fault, 0, True)
        if self.fault_desc != self.last_fault_desc:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_fault_desc, self.fault_desc, 0, True)
        if self.event != self.last_event:
            mqtt_client.publish(self.mqtt_prefix+ self.mqtt_event, self.event, 0, True)
        if self.event_desc != self.last_event_desc:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_event_desc, self.event_desc, 0, True)
        if self.auto_mode != self.last_auto_mode:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_auto_mode, self.auto_mode, 0, True)

        if self.utility_present != self.last_utility_present:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_utility_present, self.utility_present, 0, True)
        if self.utility_connected != self.last_utility_connected:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_utility_connected, self.utility_connected, 0, True)
        if self.running != self.last_running:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_running, self.running, 0, True)
        if self.exercising != self.last_exercising:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_exercising, self.exercising, 0, True)
        if self.standby != self.last_standby:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_standby, self.standby, 0, True)
        if self.action_required != self.last_action_required:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_action_required, self.action_required, 0, True)
        if self.status != self.last_status:
            mqtt_client.publish(self.mqtt_prefix+self.mqtt_status, self.status, 0, True)

    def subscribe_mqtt(self, mqtt_client):
        mqtt_client.message_callback_add(self.mqtt_prefix+self.mqtt_engine_exercise, self.on_message_engine_exercise)
        mqtt_client.subscribe(self.mqtt_prefix+self.mqtt_engine_exercise)

        mqtt_client.message_callback_add(self.mqtt_prefix+self.mqtt_standby_enable, self.on_message_standby_enable)
        mqtt_client.subscribe(self.mqtt_prefix+self.mqtt_standby_enable)

        mqtt_client.message_callback_add(self.mqtt_prefix+self.mqtt_engine_start, self.on_message_engine_start)
        mqtt_client.subscribe(self.mqtt_prefix+self.mqtt_engine_start)

    def on_message_engine_exercise(self, mosq, obj, msg):
        msg.payload = msg.payload.decode("utf-8")
        if msg.payload == "True":
            self.engine_exercise()
        elif msg.payload == "False":
            self.engine_stop()

    def on_message_standby_enable(self, mosq, obj, msg):
        msg.payload = msg.payload.decode("utf-8")
        if msg.payload == "True":
            self.standby_enable()
        elif msg.payload == "False":
            self.standby_disable()

    def on_message_engine_start(self, mosq, obj, msg):
        msg.payload = msg.payload.decode("utf-8")

        if msg.payload == "True":
            self.engine_start()
        elif msg.payload == "False":
            self.engine_stop()

def get_local_time():
    now=datetime.now()
    tz = pytz.timezone('America/New_York')
    now = tz.localize(now)
    return now

def time_sync(generator, time_delay):
    time_start = time.time()
    while _RUNNING:
        time_now = time.time()
        time_diff = time_now - time_start
        if  time_diff >= time_delay:
            generator.check_time(5,True)
            time_start = time.time()

class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass
 
 
def service_shutdown(signum, frame):
    logger.debug('Caught signal %d' % signum)
    _RUNNING = False
    raise ServiceExit

def main():
    try:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Register the signal handlers
        signal.signal(signal.SIGTERM, service_shutdown)
        signal.signal(signal.SIGINT, service_shutdown)

        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)

        parser.add_argument('-c', '--config', default='./config.ini', help='The path to the config file')
        parser.add_argument('-d', '--debug',  action='store_true', default=False, help='Prints out log')

        args = parser.parse_args()
        if args.debug:
            logger.setLevel(logging.DEBUG)
        logger.debug('Logger Started')

        config = configparser.ConfigParser()
        config.read(args.config)
        logger.debug('Connecting to Generator')
        cummins = Generator(config['CUMMINS']['Host'], config['CUMMINS']['Username'], config['CUMMINS']['Password'])
        mqtt_client = mqtt.Client("cummins")
        logger.debug('Starting Time Thread')
        time_thread = multiprocessing.Process(target=time_sync, args=(cummins,int(config['CUMMINS']['TimeSyncMin'])*60))
        mqtt_client.connect(config['MQTT']['Host'])
        cummins.subscribe_mqtt(mqtt_client)
        mqtt_client.loop_start()

        try:
            time_thread.start()

            while True:
                time.sleep(1)
                cummins.publish_mqtt(mqtt_client)

        except ServiceExit:
            _RUNNING = False
            time_thread.join()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        logger.debug('Exiting main program')
    except:
        sys.exit()

if __name__ == "__main__":
    main()
