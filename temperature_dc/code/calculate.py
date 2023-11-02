# ----------------------------------------------------------------------
#
#    Temperature Monitoring (Basic solution) -- This digital solution enables, measures,
#    reports and records different  types of temperatures (ambient, process, equipment)
#    so that the temperature conditions surrounding a process can be understood and 
#    taken action upon. This version can work for 4 types of temperature sensors (now)
#    which include k-type, RTD, ambient (AHT20), and NIR-based sensors. 
#    The solution provides a Grafana dashboard that 
#    displays the temperature timeseries, set threshold value, and a state timeline showing 
#    the chnage in temperature. An InfluxDB database is used to store timestamp, temperature, 
#    threshold and status. 
#
#
#    Copyright (C) 2022  Shoestring and University of Cambridge
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, version 3 of the License.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see https://www.gnu.org/licenses/.
#
# ----------------------------------------------------------------------

import logging
import math
from smbus2 import SMBus
from mlx90614 import MLX90614
from w1thermsensor import W1ThermSensor
import max6675
import adafruit_ahtx0
import board



logger = logging.getLogger("main.measure.conversion")

class MLX90614_temp:
    def __init__(self):
        self.bus = SMBus(1)
        self.sensor=MLX90614(self.bus,address=0x5a)

    def ambient_temp(self):
        return self.sensor.get_amb_temp()

    def object_temp(self):
        return self.sensor.get_obj_temp()
        


class sht30:
    def __init__(self):
        self.bus = SMBus(1)
        self.bus.write_i2c_block_data(0x44, 0x2C, [0x06])
        time.sleep(0.5)
        self.data = self.bus.read_i2c_block_data(0x44, 0x00, 6)

    def ambient_temp(self):
        self.temp = self.data[0] * 256 + self.data[1]
        return -45 + (175 * self.temp / 65535.0)



class W1Therm:
    def __init__(self):
        self.sensor = W1ThermSensor()
        

    def ambient_temp(self):
        return self.sensor.get_temperature()


class k_type:
    # https://github.com/archemius/MAX6675-Raspberry-pi-python/blob/master/temp_read_1_sensor.py
    def __init__(self):
        self.cs = 23
        self.sck = 24
        self.so = 25
        max6675.set_pin(self.cs, self.sck, self.so, 1) #[unit : 0 - raw, 1 - Celsius, 2 - Fahrenheit]
        

    def ambient_temp(self):
        return max6675.read_temp(self.cs)



class aht20:
    def __init__(self):
        # self.bus = SMBus(1)
        # self.sensor=adafruit_ahtx0.AHTx0(self.bus,address=0x38)
        i2c = board.I2C()
        self.sensor = adafruit_ahtx0.AHTx0(i2c)
        
    def ambient_temp(self):
        return self.sensor.temperature
         

class TemperatureMonitoringCalculation:

    def __init__(self, config):
        # calculation_conf = config['calculation']
        # self.AmplifierGain = calculation_conf['amplifier_gain']
        # self.CTRange = calculation_conf['current_range']
        # self.phases = calculation_conf['phases']
        # self.lineVoltage = calculation_conf['voltage']
        self.


    def do_run(conf):
    
        # machine_name = conf['machine'].get('name',"Machine Name Not Set")
        
        Threshold = conf['threshold']['t1']                # User sets the threshold in the config file
        
        if conf['sensing']['adc'] == 'MLX90614_temp':
            adc = MLX90614_temp()
        elif conf['sensing']['adc'] == 'W1ThermSensor':
            adc = W1Therm()
        elif conf['sensing']['adc'] == 'K-type':
            adc = k_type()
        elif conf['sensing']['adc'] == 'AHT20':
            adc = aht20()
        # elif conf['sensing']['adc'] == 'SHT30':
        #     adc = sht30()
        else:
            raise Exception(f'ADC "{conf["sensing"]["adc"]}" not recognised/supported')
        
        
        #todo: error check on loaded in config
    
        # while True:
            
        AmbientTemp = adc.ambient_temp()
        print(AmbientTemp)
        # ObjectTemp = adc.object_temp()
        if AmbientTemp > float(Threshold):
            AlertVal = 1
        else:
            AlertVal = 0

            




    def calculate(self, ADCAverageVoltage):
        AmplifierVoltageIn = ADCAverageVoltage / self.AmplifierGain
        CTClampCurrent = AmplifierVoltageIn * self.CTRange
        RMSCTClampCurrent = CTClampCurrent * self.one_over_sqrt_2

        PowerValue = self.phases * RMSCTClampCurrent * self.lineVoltage
        logger.info(f"temperature_reading: {AmbientTemp}")
        logger.debug(f"Temp: {AmbientTemp} Threshold: {Threshold} AlertStatus: {AlertVal}")
        return {"Temperature": str(AmbientTemp), "AlertStatus": str(AlertVal)}

