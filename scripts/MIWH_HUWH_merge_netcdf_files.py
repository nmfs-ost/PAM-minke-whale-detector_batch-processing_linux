''' Documentation


'''

from ecosound.core.annotation import Annotation
from ecosound.core.measurement import Measurement
import os



# Load all netCDF files
detec_files_dir = '/output'
print('Loading all .nc files in detec_files_dir')
detec = Annotation()
detec.from_netcdf(detec_files_dir, verbose=True)
print(detec.summary())

# save master nc file:
print('Saving merged detections to the file detections_dataset.nc')
detec.to_netcdf(os.path.join(detec_files_dir,'detections_dataset.nc'))

print('s')