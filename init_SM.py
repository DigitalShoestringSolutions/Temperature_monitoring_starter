# init_SM.py
# initialisation scipt run on download of this Service Module

# Get location of this script

# Take init action:

# The following should probably be done within docker/volumes/environment variables,
#   but not being a docker expert the way I can get working now is using python.
#
# The purpose is to make the UserConfig/sensor_config.toml file contents 
#   available to the ServiceModules/Sensing/temperature_dc docker container.
# Copy Solution's user config file to within the container:

from pathlib import Path
import shutil

# Note that due to the dubious exec(f.read()) that actually runs this script, 
#   Path(__file__) returns the Assembly/ShoestringAssembler location!

copy_from = str(Path(__file__).parents[3].joinpath("UserConfig/sensor_config.toml"))
copy_to = str(Path(__file__).parents[2].joinpath("Sensing/temperature_dc/config/sensor_config.toml"))

shutil.copyfile(copy_from, copy_to)

# A forseeable problem with this approach is that the config file must be edited before this script is run,
# If the userconfig is edited after the service modules have been downloaded, it won't be loaded. 
# This must be improved!