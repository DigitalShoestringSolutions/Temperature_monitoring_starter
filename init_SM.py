# init_SM.py
# initialisation scipt run on download of this Service Module

# Get location of this script

# Take init action:

# The following should probably be done within docker/volumes/environment variables,
#   but not being a docker expert the way I can get working now is using python.
#
# The purpose is to make the UserConfig/sensor_config.toml file contents 
#   available to the ServiceModules/Sensing/temperature_dc docker container.


# Create hard nonsymbolic Unix link between the file in UserConfig and the SM:

from pathlib import Path
import os

# Note that due to the dubious exec(f.read()) that actually runs this script, 
#   Path(__file__) returns the Assembly/ShoestringAssembler location!

link_from_abs = Path(__file__).parents[3].joinpath("UserConfig/sensor_config.toml")
link_to_abs = Path(__file__).parents[2].joinpath("Sensing/temperature_dc/config/sensor_config.toml")

os.system("ln " + str(link_from_abs) + " " + str(link_to_abs))
