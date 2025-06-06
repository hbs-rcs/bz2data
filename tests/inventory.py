from bz2data import DataManager, get_logger
from glob import glob
import pandas as pd
import time

if __name__ == '__main__':
    
    src_df = pd.read_csv('/path/to/user_access_keys.csv')
    src_key_id = src_df['Access key ID'][0]
    src_key = src_df['Secret access key'][0]
    source_bucket = 'webmunk-asin-files'
    names = 'source-bucket'

    data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD', njobs = 1, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log')

    data_manager.sourceBucket(src_key_id, src_key, source_bucket)
    data_manager.destinationPath('Desktop/zipped')

    inventory_dir = '/path/to/extracted/zip_lists'
    all_csvs = glob(f'{inventory_dir}/**/*.csv', recursive=True)

    t = time.time()

    # Because each inventory list may have millions of records,
    # it is best to iterate one by one as concatenating them or
    # loading them at once may use a lot more memory and note that
    # this is prior downloading any files. Inventory also works for
    # bucket to bucket transfer, see compress.py in test dir and
    # modify this code accordingly.

    for csv_inventory in all_csvs:
        data_manager.compress('download', inventory = csv_inventory)

    total_t = time.time() - t
