
# BZ2DATA

Compress data into bz2 files


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

    key_id_1 = 'KEY-ID-1'
    key_1 = 'KEY-1'
    source_bucket = 'SOURCE-BUCKET-NAME'

    key_id_2 = 'KEY-ID-2'
    key_2 = 'KEY-2'
    destination_bucket = 'DESTINATION-BUCKET-NAME'

    data_manager = bz2data.DataManager()
    
    data_manager.sourceBucket(key_id_1, key_1, source_bucket)

    data_manager.destinationBucket(key_id_2, key_2, destination_bucket)

    data_manager.transfer()


Info:

Change zip file size (default: 5000000000)

    data_manager = bz2data.DataManager(zip_size = 5000000001)

Change destination bucket storage class (default: 'STANDARD')

    data_manager = bz2data.DataManager(destination_class = 'DEEP_ARCHIVE')

    S3 storage classes:

    'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'|'SNOW'|'EXPRESS_ONEZONE'
 
Destination zip files will be counted and this count digit 
will be appended to the name, the top level directory
of the last digit count will be used when the archive has reached
zip_size and is saved; this is because if top level directories are too small 
in size it will most likely not create big enough destination files (default: 5GB).
You must be aware that top level directories contain a lot of files
or are big in size as this module tries to maximize destination file size as well as 
per object operations when saving, to change this default behavior use

    data_manager = bz2data.DataManager(merge_dirs = False) 
