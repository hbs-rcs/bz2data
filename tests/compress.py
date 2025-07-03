import pandas as pd
from bz2data import DataManager
import time

if __name__ == '__main__':
    
    src_df = pd.read_csv('/path/to/user_access_keys.csv')
    dest_df = pd.read_csv('/path/to/user_access_keys2.csv')

    src_key_id = src_df['Access key ID'][0]
    src_key = src_df['Secret access key'][0]
    source_bucket = 'source-bucket'
    source_region = 'us-east-1'

    dest_key_id = dest_df['Access key ID'][0]
    dest_key = dest_df['Secret access key'][0]
    destination_bucket = 'destination-bucket'
    destination_region = 'us-east-1'
    
    names = 'research-data-archive'

    data_manager = DataManager(archive_names = names, destination_class = 'STANDARD', njobs = 1, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log')

    data_manager.sourceBucket(src_key_id, src_key, source_bucket, region = destination_region)
    data_manager.destinationBucket(dest_key_id, dest_key, destination_bucket, region = source_region)

    t = time.time()
    data_manager.compress('compress')
    total_t = time.time() - t
