from bz2data import DataManager, get_logger
import pandas as pd
import time

if __name__ == '__main__':

    src_df = pd.read_csv('/path/to/user_access_keys.csv')
    src_key_id = src_df['Access key ID'][0]
    src_key = src_df['Secret access key'][0]
    source_bucket = 'bucket-name'
    source_region = 'us-east-1'
    
    names = 'research-data-archive'

    data_manager = DataManager(archive_names = names, destination_class = 'STANDARD', njobs = 1, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log')

    data_manager.sourceBucket(src_key_id, src_key, source_bucket, region = source_region)
    data_manager.destinationPath('Desktop/zipped')

    t = time.time()
    data_manager.compress('download')
    total_t = time.time() - t
