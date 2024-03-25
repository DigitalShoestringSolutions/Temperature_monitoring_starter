# ----------------------------------------------------------------------
#
#    Temperature Monitoring (Basic solution) -- This digital solution enables, measures,
#    reports and records different  types of temperatures (contact, air, radiated)
#    so that the temperature conditions surrounding a process can be understood and 
#    taken action upon. Suppored sensors include 
#    k-type thermocouples, RTDs, air samplers, and NIR-based sensors.
#    The solution provides a Grafana dashboard that 
#    displays the temperature timeseries, set threshold value, and a state timeline showing 
#    the chnage in temperature. An InfluxDB database is used to store timestamp, temperature, 
#    threshold and status. 
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

from grove.adc import ADC as GroveADC
import logging

logger = logging.getLogger("main.measure.adc.grove")

class ADC:
    def __init__(self, config):
        if config['computing'] and config['computing']['hardware'] == "Rock4C+":
            self.adc = GroveADC(bus=7)
        else:
            self.adc = GroveADC()
        self.channel = config['adc']['channel']
        self.ADCMax = pow(2, 12) - 1
        self.ADCVoltage = 3.3

    def sample(self):
        voltage = self.adc.read_voltage(self.channel) / 1000
        logger.debug(f"v {voltage}")
        return voltage
