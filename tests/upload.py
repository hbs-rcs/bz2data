
from glob import glob
import pandas as pd
import bz2data

dest_df = pd.read_csv('/path/to/user_access_keys.csv')

dest_key_id = dest_df['Access key ID'][0]
dest_key = dest_df['Secret access key'][0]
destination_bucket = 'bucket-name'

names = 'research-data-archive'

data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD', njobs = 12, log_file = 'bz2data-research-data.log', error_log = 'bz2data-error.log')

data_manager.destinationBucket(dest_key_id, dest_key, destination_bucket)

# For really large datasets it make take some time for glob to upload to memory, iglob doesn't work because
# the number of total files needs to be counted to process in pages therefore dividing the directories is recommended

all_directories = glob('/path/to/dataset/folders/*')

for dir_path in all_directories:

    with open('project-dirs-progress.log', 'a') as fd:
        fd.write(f'{dir_path}\n')
        
    current_dir = dir_path.split('/')[-1]
    zip_dir = f'{names}-{current_dir}'
    # self.zip_name is the internal object names for zip files
    data_manager.zip_name = zip_dir
    data_manager.sourcePath(dir_path)
    data_manager.compress('upload')
