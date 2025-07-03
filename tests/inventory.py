
from glob import glob
import pandas as pd
import bz2data
import time

if __name__ == '__main__':
    src_df = pd.read_csv('/path/to/user_access_keys.csv')
    src_key_id = src_df['Access key ID'][0]
    src_key = src_df['Secret access key'][0]
    source_bucket = 'bucket-name'
    source_region = 'us-east-1'
    
    names = 'research-data-archive'

    data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD', njobs = 32, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log', timeout = 0)

    data_manager.sourceBucket(src_key_id, src_key, source_bucket, region = source_region)
    data_manager.destinationPath('/large/capacity/volume/mountpoint/zipped')

    inventory_dir = '/path/to/extracted/zip_lists'
    all_csvs = glob(f'{inventory_dir}/**/*.csv', recursive=True)

    t = time.time()
    
    # Because each inventory list may have millions of records,
    # it is best to iterate one by one as concatenating them or
    # loading them at once may use a lot more memory and note that
    # this is prior downloading any files. Inventory also works for
    # bucket to bucket transfer, see compress.py in test dir and
    # modify this code accordingly.
    
    for group, csv_inventory in enumerate(all_csvs):
        group_str = str(group)
        zip_group = f'{names}-group{group-str}'
        with open('./csv-progress.log', 'a') as fd:
            fd.write(f'{csv_inventory}\n')
        # Add csv file index as group number to the zip names (example: research-data-archive-group1-0.zip)
        data_manager.zip_name = zip_group
        
        data_manager.compress('download', inventory = csv_inventory)
        
    total_t = time.time() - t
