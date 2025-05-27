from bz2data import get_logger
import pandas as pd
import bz2data
import time

dest_df = pd.read_csv('/path/to/user_accessKeys.csv')


dest_key_id = dest_df['Access key ID'][0]
dest_key = dest_df['Secret access key'][0]
destination_bucket = 'bucket-name'

names = 'research-data-archive'

data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD', log_file = './bz2data-research-data.log', timeout = 3)

data_manager.sourcePath('Desktop/unzipped')

data_manager.destinationBucket(dest_key_id, dest_key, destination_bucket)

data_manager.compress('upload')


