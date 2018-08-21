## License: Apache 2.0. See LICENSE file in root directory.
## Copyright(c) 2015-2017 Intel Corporation. All Rights Reserved.

#####################################################
## librealsense tutorial #1 - Accessing depth data ##
#####################################################

# First import the library
import pyrealsense2
from pyrealsense2 import pyrealsense2 as rs
import time
import numpy as np
import cv2 
from cv2 import cv2
import json

class DataObject(object):
    def __init__(self, class_name, distance):
        self.class_name = class_name
        self.distance = distance

def jsonDefault(object):
    return object.__dict__

class librealsense_object_detect(object):

    def __init__(
        self, 
        send_to_hub_callback = None):
        self.send_to_hub_callback = send_to_hub_callback

    def start(self):
        try:
            # Setup:
            pipe = rs.pipeline()

            #configure profiles 
            config = rs.config()
            config.enable_stream(rs.stream.depth, 640, 480, rs.format.z16, 6)
            config.enable_stream(rs.stream.color, 640, 480, rs.format.rgb8, 6)

            profile = pipe.start(config)

            # Skip 5 first frames to give the Auto-Exposure time to adjust
            for x in range(5):
                pipe.wait_for_frames()

            while True:
                # Store next frameset for later processing:
                frameset = pipe.wait_for_frames()
                color_frame = frameset.get_color_frame()
                color = np.asanyarray(color_frame.get_data())

                # Standard OpenCV boilerplate for running the net:# Stand
                height, width = color.shape[:2]
                expected = 300
                aspect = width / height
                resized_image = cv2.resize(color, (int(round(expected * aspect)), expected))
                crop_start = int(round(expected * (aspect - 1) / 2))
                crop_img = resized_image[0:expected, crop_start:crop_start + expected]

                net = cv2.dnn.readNetFromCaffe("MobileNetSSD_deploy.prototxt", "MobileNetSSD_deploy.caffemodel")
                in_scale_factor = 0.007843
                mean_val = 127.53
                class_names = ("background", "aeroplane", "bicycle", "bird", "boat",
                                "bottle", "bus", "car", "cat", "chair",
                                "cow", "diningtable", "dog", "horse",
                                "motorbike", "person", "pottedplant",
                                "sheep", "sofa", "train", "tvmonitor")

                blob = cv2.dnn.blobFromImage(crop_img, in_scale_factor, (expected, expected), mean_val, False)
                net.setInput(blob, "data")
                detections = net.forward("detection_out")

                label = detections[0, 0, 0, 1]
                xmin = detections[0, 0, 0, 3]
                ymin = detections[0, 0, 0, 4]
                xmax = detections[0, 0, 0, 5]
                ymax = detections[0, 0, 0, 6]

                class_name = class_names[int(label)]

                align = rs.align(rs.stream.color)
                frameset = align.process(frameset)
                aligned_depth_frame = frameset.get_depth_frame()
                depth = np.asanyarray(aligned_depth_frame.get_data())

                # Crop depth data:
                scale = height / expected
                xmin_depth = int((xmin * expected + crop_start) * scale)
                ymin_depth = int((ymin * expected) * scale)
                xmax_depth = int((xmax * expected + crop_start) * scale)
                ymax_depth = int((ymax * expected) * scale)
                depth = depth[xmin_depth:xmax_depth, ymin_depth:ymax_depth].astype(float)

                # Get data scale from the device and convert to meters
                depth_scale = profile.get_device().first_depth_sensor().get_depth_scale()
                depth = depth * depth_scale
                dist, _, _, _ = cv2.mean(depth)
                print("Detected a {0} {1:.3} meters away.".format(class_name, dist))

                json_object = json.dumps(DataObject(class_name, dist), default=jsonDefault)
                self.send_to_hub_callback(json_object)

        except Exception as e:
            print(e)
            pass
            
