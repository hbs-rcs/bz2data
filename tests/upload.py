from bz2data import DataManager, get_logger
import pandas as pd
import time

if __name__ == '__main__':

    dest_df = pd.read_csv('/path/to/user_access_keys.csv')
    dest_key_id = dest_df['Access key ID'][0]
    dest_key = dest_df['Secret access key'][0]
    destination_bucket = 'bucket-name'
    names = 'research-data-archive'
    
    data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD', njobs = 1, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log')
    
    data_manager.sourcePath('Desktop/unzipped')
    data_manager.destinationBucket(dest_key_id, dest_key, destination_bucket)
    
    t = time.time()
    data_manager.compress('upload')
    total_t = time.time() - t
