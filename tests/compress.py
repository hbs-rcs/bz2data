import pandas as pd
import bz2data
import time

src_df = pd.read_csv('/path/to/user_accessKeys.csv')

dest_df = pd.read_csv('/path/to/user_accessKeys2.csv')

src_key_id = src_df['Access key ID'][0]
src_key = src_df['Secret access key'][0]
source_bucket = 'source-bucket'

dest_key_id = dest_df['Access key ID'][0]
dest_key = dest_df['Secret access key'][0]
destination_bucket = 'destination-bucket'

names = 'research-data-archive'

data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD', njobs = 7, log_file = './bz2data-research-data.log', timeout = 0)

data_manager.sourceBucket(src_key_id, src_key, source_bucket)

data_manager.destinationBucket(dest_key_id, dest_key, destination_bucket)

t = time.time()
data_manager.compress('compress')
total_t = time.time() - t
