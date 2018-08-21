# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.
import json
import time
import sys
import os
import iothub_client
from iothub_client import iothub_client as ic
import lrs_capture 
import requests

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubModuleClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000

# global counters
RECEIVE_CALLBACKS = 0
SEND_CALLBACKS = 0

# Callback received when the message that we're forwarding is processed.
def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    SEND_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_CALLBACKS )

def send_to_hub_callback(strMessage):
    message = ic.IoTHubMessage(bytearray(strMessage))
    #message = ic.IoTHubMessage(bytearray(strMessage, 'utf8'))
    hub_manager.forward_event_to_output("streamOutput", message, 0)

class HubManager(object):
    def __init__(
            self,
            protocol = ic.IoTHubTransportProvider.MQTT):
        self.client_protocol = protocol
        self.client = ic.IoTHubModuleClient()
        self.client.create_from_environment(protocol)

        # set the time until a message times out
        self.client.set_option("messageTimeout", MESSAGE_TIMEOUT)

    # Forwards the message received onto the next stage in the process.
    def forward_event_to_output(self, outputQueueName, event, send_context):
        self.client.send_event_async(
            outputQueueName, event, send_confirmation_callback, send_context)

if __name__ == '__main__':
    try:
        print ( "\nPython %s\n" % sys.version )
        print ( "IoT Hub Client for Python" )

        global hub_manager
        PROTOCOL = ic.IoTHubTransportProvider.MQTT
        hub_manager = HubManager(PROTOCOL)

        print ( "Starting the IoT Hub Python sample using protocol %s..." % hub_manager.client_protocol )
        print ( "The sample is now waiting for messages and will indefinitely.  Press Ctrl-C to exit. ")

        lrs_capture.librealsense_object_detect(send_to_hub_callback).start()

    except ic.IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
    except KeyboardInterrupt:
        print ( "IoTHubModuleClient sample stopped" )
