from csv import reader
import csv
from datetime import datetime
import logging
import time
import random
import threading
from venv import logger
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
import config
from domain.parking import Parking

stop_thread_flag = False  
output_array = []
output_array2 = []

class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename

    def read_csv_file1_to_array(self):
        data_array = []
        with open(self.accelerometer_filename, 'r', newline='') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                for item in row:
                    data_array.append(item)
        return data_array
        
    
    def read_csv_file2_to_array(self):
        data_array = []
        with open(self.gps_filename, 'r', newline='') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                for item in row:
                    data_array.append(item)
        return data_array
    
    def read_csv_file3_to_array(self):
            data_array = []
            with open(self.parking_filename, 'r', newline='') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    for item in row:
                        data_array.append(item)
            return data_array

    def process_data(self, input_data, input_data2, input_data3, output_array, output_array2):
       global stop_thread_flag
       temp_arr = []
       temp_arr2 = []

       while not stop_thread_flag:
            index = random.randint(0, len(input_data) - 1)
            index2 = random.randint(0, len(input_data3) - 1)

            while index % 3 != 0:
                index = random.randint(0, len(input_data) - 1)
            if index % 3 == 0:
                temp_arr.extend(input_data[index:index + 3])

            while index2 % 3 != 0:
                index2 = random.randint(0, len(input_data3) - 1)
            if index2 % 3 == 0:
                temp_arr2.extend(input_data3[index2:index2 + 3])

            temp = random.randint(0, len(input_data2) - 1)
            while temp % 2 != 0:
                temp = random.randint(0, len(input_data2) - 1)
            if temp % 2 == 0:
                temp_arr.extend(input_data2[temp:temp + 2])
            if index % 3 == 0 and temp % 2 == 0:
                output_array.append(temp_arr)
                temp_arr = []
            if index2 % 3 == 0:
                output_array2.append(temp_arr2)
                temp_arr2 = []
            time.sleep(1)

    def stopReading(self):
        global stop_thread_flag
        stop_thread_flag = True

    def read(self, output_array) -> AggregatedData:             
        return AggregatedData(
            Accelerometer(int(output_array[-1:][0][0]), int(output_array[-1:][0][1]), int(output_array[-1:][0][2])),
            Gps(float(output_array[-1:][0][3]), float(output_array[-1:][0][4])),
            datetime.now(),
            config.USER_ID,
        )    
    
    def read_par(self, output_array2) -> Parking:         
        return Parking(
            int(output_array2[-1:][0][0]),
            Gps(float(output_array2[-1:][0][1]), float(output_array2[-1:][0][2])),
        ) 

    def startReading(self, input_array, input_array2, input_array3):
        output_array = []
        output_array2 = []

        data_thread = threading.Thread(target=self.process_data, args=(input_array, input_array2, input_array3, output_array, output_array2))
        data_thread.start()
        return data_thread, output_array, output_array2
    

   

    