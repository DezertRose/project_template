from csv import reader
import csv
from datetime import datetime
import time
import random
import threading
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
import config

stop_thread_flag = False  
output_array = []

class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename

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
    
    def process_data(self, input_data, input_data2, output_array):
        global stop_thread_flag
        temp_arr = []
        while not stop_thread_flag:
            index = random.randint(0, len(input_data) - 1)
            while index % 3 != 0:
                index = random.randint(0, len(input_data) - 1)
            if index % 3 == 0:
                temp_arr.extend(input_data[index:index + 3])

            temp = random.randint(0, len(input_data2) - 1)
            while temp % 2 != 0:
                temp = random.randint(0, len(input_data2) - 1)
            if temp % 2 == 0:
                temp_arr.extend(input_data2[temp:temp + 2])
            if index % 3 == 0 and temp % 2 == 0:
                output_array.append(temp_arr)
                temp_arr = []

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

    def startReading(self, input_array, input_array2):
        output_array = []
        data_thread = threading.Thread(target=self.process_data, args=(input_array, input_array2, output_array))
        data_thread.start()
        return data_thread, output_array
    

   

    