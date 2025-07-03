
# BZ2DATA

Zip archive S3 data, at least v4.6 to extract, compression method=bzip2


Build:

	python3 -m pip install --upgrade build (Unix/macOS)
	
    py -m pip install --upgrade build (Windows)

    Run from package directory:
    
    python3 -m build (Unix/macOS)
	
    py -m build (Windows)

Source tar ball and wheel distribution will be saved in dist folder

Both source and distribution can be installed with pip

	pip install bz2data-0.0.1.tar.gz	

	pip install bz2data-0.0.1-py3-none-any.whl


Usage:

    from bz2data import DataManager

    if __name__ == '__main__':

        src_key_id = 'SRC-KEY-ID'
        src_key = 'SRC-KEY'
        source_bucket = 'SOURCE-BUCKET-NAME'
        source_region = 'us-east-1'

        dest_key_id = 'DEST-KEY-ID'
        dest_key = 'DEST-KEY'
        destination_bucket = 'DESTINATION-BUCKET-NAME'
        destination_region = 'us-east-1'
        
        names = 'research-data-archive'

        data_manager = bz2data.DataManager(archive_names = names, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log')
        
        data_manager.sourceBucket(src_key_id, src_key, source_bucket, region = source_region)

        data_manager.destinationBucket(dest_key_id, dest_key, destination_bucket, region = destination_region)

        data_manager.compress('compress')


Info:

Policy examples in tests/policies

For bucket to bucket add the user.json policy to the user on the source account, bucket.json policy to the destination bucket policy and default.json to the user on the destination account

For upload or dowload add the default.json policy to the user on the source/destination account

Will overwrite .aws/credentials and .aws/config using credential csv files provided when transferring bucket to bucket

Change zip file size (default: 5000000000)

    data_manager = bz2data.DataManager(zip_size = 5000000001, archive_names = names)
    
Change max file size compression (default: 9000000000, bigger files will be transferred without compression)

    data_manager = bz2data.DataManager(max_file_size = 8999999999, archive_names = names)

Change destination bucket storage class (default: 'STANDARD')

    data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD_IA')

    S3 storage classes:

    'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'|'SNOW'|'EXPRESS_ONEZONE'
    
    Only STANDARD and STANDARD_IA have been tested, other classes like GLACIER will require additional code
 
Destination zip file names will have a count digit appended to the 
string passed as the 'archive_names' parameter for each zip archive created when 
zip file zise is reached, for example:

    research-data-archive-0.zip

Upload and compress from disk

    data_manager = bz2data.DataManager(archive_names = names, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log')
    
    data_manager.sourcePath('Desktop/unzipped')

    data_manager.destinationBucket(dest_key_id, dest_key, destination_bucket)

    data_manager.compress('upload')

Download and compress to disk

    data_manager = bz2data.DataManager(archive_names = names, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log')
    
    data_manager.destinationPath('Desktop/zipped')

    data_manager.sourceBucket(src_key_id, src_key, source_bucket)

    data_manager.compress('download')

Resume transfer

    You can resume a transfer that was interrupted by passing the original log as the resume option string
    
    It will also not overwrite already downloaded and zipped files even if you change the zip size on resume
    
    Once the log reaches three times the zip size it will create a new one, the old ones will have digits appdended to the name
    
    data_manager = bz2data.DataManager(archive_names = names, log_file = './new-bz2data.log', error_log = './bz2data-error.log')
    
    data_manager.compress('compress', resume = './bz2data.log')

Run in parallel

    data_manager = bz2data.DataManager(archive_names = names, njobs = 10)
    
S3 inventory list

    Please refer to the inventory script in the test directory, using S3 inventory will drastically
    reduce listing pricing as the cost is $0.0025 per million objects listed, this is recommended for 
    extremely large datasets that otherwise would drive up the cost significantly with the list v2 api
    Inventory files must be created in csv format from the AWS web console
    
    data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD', njobs = 10, log_file = './bz2data-research-data.log', error_log = './bz2data-error.log', timeout = 0)
    
    data_manager.compress('download', inventory = 'inventory-file.csv')

Add delay between uploads/downloads to prevent flooding the network (Most of the time it is not necessary)

    data_manager = bz2data.DataManager(archive_names = names, timeout = 0)

Extract using command line

    Most operating systems built in zip utility will extract by double clicking but 7zip will work on CLI as well
    
    7za x research-data-archive-0.zip
