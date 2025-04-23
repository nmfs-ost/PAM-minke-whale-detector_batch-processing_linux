''' Documentation


'''

from ecosound.core.annotation import Annotation
from ecosound.core.measurement import Measurement
import os

#feed in ENV variable
audio_dir = sys.argv[1]
audio_dir = audio_dir.replace("/","\\")

# Load all netCDF files
detec_files_dir = '/output'
print('Loading all .nc files in detec_files_dir')
detec = Annotation()
detec.from_netcdf(detec_files_dir, verbose=True)
print(detec.summary())

#hardcode the output path of the aggregated file to the pam-ww which is the current intended analysis location. 

# update audio files path:
audio_files_dir = f"C:\\pamdata_mount\\{audio_dir)}"
print('Updating path of the audio files')
detec.update_audio_dir(audio_files_dir)

# save master nc file:
print('Saving merged detections to the file detections_dataset.nc')
detec.to_netcdf(os.path.join(detec_files_dir,'detections_dataset.nc'))

print('s')