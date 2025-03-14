
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

    import bz2data

    src_key_id = 'SRC-KEY-ID'
    src_key = 'SRC-KEY'
    source_bucket = 'SOURCE-BUCKET-NAME'

    dest_key_id = 'DEST-KEY-ID'
    dest_key = 'DEST-KEY'
    destination_bucket = 'DESTINATION-BUCKET-NAME'
    
    names = 'research-data-archive'

    data_manager = bz2data.DataManager(archive_names = names)
    
    data_manager.sourceBucket(src_key_id, src_key, source_bucket)

    data_manager.destinationBucket(dest_key_id, dest_key, destination_bucket)

    data_manager.compress()


Info:

Change zip file size (default: 5000000000)

    data_manager = bz2data.DataManager(zip_size = 5000000001, archive_names = names)

Change destination bucket storage class (default: 'STANDARD')

    data_manager = bz2data.DataManager(archive_names = names, destination_class = 'STANDARD_IA')

    S3 storage classes:

    'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'|'SNOW'|'EXPRESS_ONEZONE'
 
Destination zip file names will have a count digit appended to the 
string passed as the 'archive_names' parameter for each zip archive created when 
zip file zise is reached, for example:

    research-data-archive-0.zip

Upload and compress from disk

    data_manager = bz2data.DataManager(archive_names = names, log_file = './bz2data.log')
    
    data_manager.sourcePath('Desktop/unzipped')

    data_manager.destinationBucket(key_id_2, key_2, destination_bucket)

    data_manager.upload()

Download and compress to disk

    data_manager = bz2data.DataManager(archive_names = names, log_file = './bz2data.log')
    
    data_manager.destinationPath('Desktop/zipped')

    data_manager.sourceBucket(key_id_1, key_1, source_bucket)

    data_manager.download()

Resume transfer

    You can resume a transfer that was interrupted by passing the original log as the resume option string
    
    data_manager = bz2data.DataManager(archive_names = names, log_file = './new-bz2data.log')
    
    data_manager.compress(resume = './bz2data.log')

Extract using command line

    Most operating systems built in zip utility will extract by double clicking but 7zip will work on CLI as well
    
    7za x research-data-archive-0.zip
