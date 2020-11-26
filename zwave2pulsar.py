#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys, os
import resource
import configparser
import signal
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger('openzwave')
import openzwave
from openzwave.network import ZWaveNetwork
from openzwave.option import ZWaveOption
from enum import Enum
from pydispatch import dispatcher
import time
import uuid
import pulsar
from pulsar.schema import *

class Reading(Record):
    """A simple floating point value."""
    value = Float()

class ZWaveNetworkState(Enum):
    """Status of the ZWave Network."""
    STOPPED = 1
    FAILED = 2
    RESET = 3
    STARTED = 4
    AWAKE = 5
    READY = 6

class GatewayState(Record):
    """The state of the network gateway."""
    state = ZWaveNetworkState

class ScriptState(Enum):
    """Status of this script execution."""
    STARTED = 1
    RUNNING = 2
    EXITED = 3
    
class ScriptInfo(Record):
    """Information about the state of the script"""
    state = ScriptState # Script state
    startup = Integer() # Epoch time that script began
    version = String() # version of this script
    
device="/dev/ttyACM0"
log="Debug"

#Define some manager options
options = ZWaveOption(device, user_path=".", cmd_line="")
options.set_log_file("OZW_Log.log")
options.set_append_log_file(False)
options.set_console_output(False)
options.set_save_log_level(log)
options.set_logging(False)
options.lock()

last_state = None

def zw_value_update(network, node, value):
    global last_state
    # If network is not ready and hasn't changed status, just return
    if network.state != last_state:
        if network.state==network.STATE_STOPPED:
            print("Network is stopped")
            gwstate_producer.send(GatewayState(state=ZWaveNetworkState.STOPPED))
        if network.state==network.STATE_FAILED:
            print("Network has failed")
            gwstate_producer.send(GatewayState(state=ZWaveNetworkState.FAILED))
        if network.state==network.STATE_RESETTED:
            print("Network has reset")
            gwstate_producer.send(GatewayState(state=ZWaveNetworkState.RESET))
        if network.state==network.STATE_STARTED:
            print("Network has started")
            gwstate_producer.send(GatewayState(state=ZWaveNetworkState.STARTED))
        if network.state==network.STATE_AWAKED:
            print("Network has awaked")
            gwstate_producer.send(GatewayState(state=ZWaveNetworkState.AWAKE))
        if network.state==network.STATE_READY:
            gwstate_producer.send(GatewayState(state=ZWaveNetworkState.READY))
    last_state = network.state
    # Regardless, if network isn't ready, then return
    if network.state!=network.STATE_READY:
        return
    # ZW095 handler
    if (node.product_id == "0x005f" and node.manufacturer_id == "0x0086"):
        if value.label == "Power" and isinstance(value.data, float):
            power_producer.send(Reading(value=value.data))
            print("sent power update ({})".format(value.data))
        if value.label == "Energy" and isinstance(value.data, float):
            energy_producer.send(Reading(value=value.data))
            print("sent energy update ({})".format(value.data))

def terminateProcess(signalNumber, frame):
    """Send a final pulsar event on script shutdown, and close all producers."""
    print ('terminating the process')
    exit_record = ScriptInfo(state=ScriptState.EXITED, startup=STARTUP, version=VERSION)
    agent_producer.send(exit_record)
    print("sent script-exit: {}".format(str(exit_record)))
    client.close()
    sys.exit()
            
if __name__ == '__main__':
    VERSION = "0.0.1"
    STARTUP = int(time.time())
    config = configparser.ConfigParser()
    config.read('zwave2pulsar.conf')
    pulsarcfg = config["pulsar"]
    pulsar_host = pulsarcfg.get("PulsarHost", "pulsar://localhost:6550")
    auth_token = pulsarcfg.get("AuthToken", None)
    power_topic = pulsarcfg.get("PowerTopic")
    energy_topic = pulsarcfg.get("EnergyTopic")
    gwstate_topic = pulsarcfg.get("GatewayStateTopic")
    agent_topic = pulsarcfg.get("AgentTopic")
    health_interval = pulsarcfg.getint("ScriptRunUpdateInterval", 60)
    # Create a network object
    network = ZWaveNetwork(options, log=None)
    # Create Producer
    if auth_token:
        client = pulsar.Client(pulsar_host,
                               authentication=AuthenticationToken(auth_token))
    else:
        client = pulsar.Client(pulsar_host)
    power_producer = client.create_producer(power_topic, schema=AvroSchema(Reading))
    energy_producer = client.create_producer(energy_topic, schema=AvroSchema(Reading))
    gwstate_producer = client.create_producer(gwstate_topic, schema=AvroSchema(GatewayState))
    agent_producer = client.create_producer(agent_topic, schema=AvroSchema(ScriptInfo))
    startup_record = ScriptInfo(state=ScriptState.STARTED, startup=STARTUP, version=VERSION)
    agent_producer.send(startup_record)
    print("sent script-startup: {}".format(str(startup_record)))
    # Start listening for variable updates on the ZWave Network
    dispatcher.connect(zw_value_update, ZWaveNetwork.SIGNAL_VALUE)
    # Orderly shutdown on TERM/INTERRUPT signals
    signal.signal(signal.SIGTERM, terminateProcess)
    signal.signal(signal.SIGINT, terminateProcess)
    # Spin and let the dispatcher work.  Periodically send health/status updates
    sleep_count = 0
    while True:
        time.sleep(1.0)
        sleep_count += 1
        if sleep_count % health_interval == 0:
                run_record = ScriptInfo(state=ScriptState.RUNNING, startup=STARTUP, version=VERSION)
                agent_producer.send(run_record)
                print("sent script-running: {}".format(str(run_record)))
