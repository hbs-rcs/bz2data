
from multiprocessing.managers import SharedMemoryManager
from boto3.s3.transfer import TransferConfig
from multiprocessing import shared_memory
import boto3.s3.transfer as s3transfer
from filelog import get_logger
import multiprocessing
from glob import glob, iglob
import pandas as pd
import botocore
import zipfile
import shutil
import boto3
import time
import io
import os

def paginate_generator(generator, page_size):
    """
    Paginates a given generator into chunks of page_size.

    Args:
        generator: The generator to paginate.
        page_size: The number of items per page.

    Yields:
        A list containing items for the current page.
    """
    page = []
    for item in generator:
        page.append(item)
        if len(page) == page_size:
            yield page
            page = []
    if page:  # Yield any remaining items in the last partial page
        yield page
        
def get_object(obj):

    page = obj['Page']
    idx = obj['Index']
    file_key = obj['Key']
    key_id = obj['KeyId']
    bucket_key = obj['BucketKey']
    bucket = obj['Bucket']
    error_log = obj['ErrorLog']
    name = obj['SharedName']
    size = obj['Size']

    source_sess = boto3.Session(aws_access_key_id = key_id, aws_secret_access_key = bucket_key)
    source_client = source_sess.client('s3')

    try:
        objshm = shared_memory.SharedMemory(name = name)
        key_obj = source_client.get_object(Bucket = bucket, Key = file_key)
        objshm.buf[:size] = key_obj['Body'].read()
        objshm.close()
    except:
        with open(error_log, 'a') as error_file:
            error_file.write(f'Error downloading {file_key}\n')

def get_file(obj):

    page = obj['Page']
    idx = obj['Index']
    file_key = obj['Key']
    error_log = obj['ErrorLog']
    size = obj['Size']
    name = obj['SharedName']

    try:
        file_descriptor = os.open(file_key, os.O_RDONLY)
        objshm = shared_memory.SharedMemory(name = name)
        objshm.buf[:size] = os.read(file_descriptor, size)
        objshm.close()
        os.close(file_descriptor)
    except:
        with open(error_log, 'a') as error_file:
            error_file.write(f'Error reading {file_key}\n')
            
class DataManager():

    def __init__(self, zip_size = 5000000000, archive_names = 'bz2data-zip-archive', destination_class = 'STANDARD', njobs = 1, log_file = 'bz2data.log', error_log = 'bz2data-error.log', max_file_size = 9000000000, multipart_threshold = 500000000, multipart_chunksize = 50000000, timeout = 0):

        '''

        S3 storage classes:
        'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'|'SNOW'|'EXPRESS_ONEZONE'

        '''

        self.log_file = log_file
        self.logger = get_logger(level = 'INFO', log_file = self.log_file)
        self.error_log = error_log
        self.log_count = 0
        self.zip_name = archive_names
        self.zip_size = zip_size
        self.source = boto3.Session
        self.destination = boto3.Session
        self.destination_class = destination_class
        self.source_bucket = ''
        self.destination_bucket = ''
        self.object_count = 0
        self.obj_size = 0
        self.file_size = 0
        self.objects = 0
        self.zip_list = []
        self.timeout = timeout
        self.source_directory = ''
        self.page_size = 1000
        self.destination_directory = ''
        self.njobs = njobs
        self.src_key_id = ''
        self.src_key = ''
        self.dest_key_id = ''
        self.dest_key = ''
        self.config = TransferConfig(multipart_threshold=multipart_threshold, max_concurrency=self.njobs, multipart_chunksize=multipart_chunksize, num_download_attempts=5, use_threads=True)
        self.max_file_size = max_file_size
        self.src_region = ''
        self.dest_region= ''

    def sourceBucket(self, key_id = '', key = '', bucket = '', region = 'us-east-1'):
        if all((key_id, key, bucket)):
            self.src_key_id = key_id
            self.src_key = key
            self.src_region = region
            source_sess = boto3.Session(
                aws_access_key_id = key_id,
                aws_secret_access_key = key
            )
            self.source = source_sess
            self.source_bucket = bucket

#           # Counting the objects can take a very long time for large datasets and there is an additional charge for listing them as well.
#            s3source = source_sess.resource('s3')
#            source_bucket = s3source.Bucket(bucket)
#            total_objs = sum(1 for _ in source_bucket.objects.all())
#            self.objects = total_objs
#            total_bytes = sum([obj.size for obj in source_bucket.objects.all()])
#            self.logger(f'\nSource:\nTotal bucket size: {total_bytes/1024/1024/1024} GB\ntotal bucket objects: {total_objs}')

    def destinationBucket(self, key_id = '', key = '', bucket = '', region = 'us-east-1'):
        if all((key_id, key, bucket)):
            self.dest_key_id = key_id
            self.dest_key = key
            self.dest_region = region
            dest_sess = boto3.Session(
                aws_access_key_id = key_id,
                aws_secret_access_key = key
            )
            self.destination = dest_sess
            self.destination_bucket = bucket

    def sourcePath(self, source_directory = '', page_size = 1000):

        if os.path.exists(source_directory):
            self.source_directory = source_directory
            self.page_size = page_size
        else:
            self.logger(f'The path {self.source_directory} does not exist.')

    def destinationPath(self, destination_directory = ''):

        if os.path.exists(destination_directory):
            self.destination_directory = destination_directory
        else:
            self.logger(f'The path {self.source_directory} does not exist.')

    def generate_zip(self, files = [], save_name = '', error_log = ''):
        if all((self.source_bucket, self.destination_bucket)):
            destination_client = self.destination.client('s3')
            zip_buffer = io.BytesIO()

            with SharedMemoryManager() as smm:
                for file_object in files:
                    object_size = file_object['Size']
                    shm_name = smm.SharedMemory(size=object_size).name
                    file_object['SharedName'] = shm_name
                pool = multiprocessing.Pool(processes=self.njobs)
                pool_map = pool.map(get_object, files)
                pool.close()
                pool.join()
                
                if os.path.getsize(self.log_file) > (self.zip_size * 3):
                    os.rename(self.log_file, '.' + self.log_file.split('.')[1] + f'-{self.log_count}' + '.log')
                    with open(self.log_file, 'w'):
                        pass
                    self.logger('BZ2DATA Harvard Business School (2025)\n')
                    
                last_page, last_idx = files[-1]['Page'], files[-1]['Index']
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:
                    
                    while files:
                        try:
                            obj = files.pop(0)
                            page = obj['Page']
                            idx = obj['Index']
                            object_path, size = obj['Key'], obj['Size']
                            name = obj['SharedName']
                            shm = shared_memory.SharedMemory(name=name)
                            zip_file.writestr(object_path, shm.buf[:size].tobytes(), compress_type = zipfile.ZIP_BZIP2)
                            shm.close()
                            shm.unlink()
                        except:
                            pass
                    zip_file.close()

                zip_buffer.seek(0)
                buffer_size = zip_buffer.getbuffer().nbytes
                destination_client.upload_fileobj(zip_buffer, self.destination_bucket, save_name, ExtraArgs = { 'StorageClass' : self.destination_class }, Config=self.config)
                self.logger(f'{last_page} {last_idx} ' + f'Transferred: {save_name} Size: {buffer_size} ' + f'Total: {self.obj_size}')

                time.sleep(self.timeout) if self.timeout else None
        else:
            print('Error source bucket and destination bucket are needed to compress')

    def upload_zip(self, files = [], save_name = '', error_log = ''):
        if all((self.source_directory, self.destination_bucket)):
            destination_client = self.destination.client('s3')
            zip_buffer = io.BytesIO()

            with SharedMemoryManager() as smm:
                for file_object in files:
                    object_size = file_object['Size']
                    shm_name = smm.SharedMemory(size=object_size).name
                    file_object['SharedName'] = shm_name
                    
                pool = multiprocessing.Pool(processes=self.njobs)
                pool_map = pool.map(get_file, files)
                pool.close()
                pool.join()
                
                if os.path.getsize(self.log_file) > (self.zip_size * 3):
                    os.rename(self.log_file, '.' + self.log_file.split('.')[1] + f'-{self.log_count}' + '.log')
                    with open(self.log_file, 'w'):
                        pass
                    self.logger('BZ2DATA Harvard Business School (2025)\n')
                    
                last_page, last_idx = files[-1]['Page'], files[-1]['Index']
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:

                    while files:
                        try:
                            obj = files.pop(0)
                            page = obj['Page']
                            idx = obj['Index']
                            object_path, size = obj['Key'], obj['Size']
                            name = obj['SharedName']
                            shm = shared_memory.SharedMemory(name=name)
                            zip_file.writestr(object_path, shm.buf[:size].tobytes(), compress_type = zipfile.ZIP_BZIP2)
                            shm.close()
                            shm.unlink()
                        except:
                            pass
                    zip_file.close()

                zip_buffer.seek(0)
                buffer_size = zip_buffer.getbuffer().nbytes

                destination_client.upload_fileobj(zip_buffer, self.destination_bucket, save_name, ExtraArgs = { 'StorageClass' : self.destination_class }, Config=self.config)
                self.logger(f'{last_page} {last_idx} ' + f'Uploaded: {save_name} Size: {buffer_size} ' + f'Total: {self.obj_size}')

                time.sleep(self.timeout) if self.timeout else None
        else:
            print('Error source directory and destination bucket are needed to upload')

    def download_zip(self, files = [], save_name = '', error_log = ''):
        if all((self.source_bucket, self.destination_directory)):
            zip_buffer = io.BytesIO()

            with SharedMemoryManager() as smm:
                for file_object in files:
                    object_size = file_object['Size']
                    shm_name = smm.SharedMemory(size=object_size).name
                    file_object['SharedName'] = shm_name

                pool = multiprocessing.Pool(processes=self.njobs)
                pool_map = pool.map(get_object, files)
                pool.close()
                pool.join()

                if os.path.getsize(self.log_file) > (self.zip_size * 3):
                    os.rename(self.log_file, '.' + self.log_file.split('.')[1] + f'-{self.log_count}' + '.log')
                    with open(self.log_file, 'w'):
                        pass
                    self.logger('BZ2DATA Harvard Business School (2025)\n')

                last_page, last_idx = files[-1]['Page'], files[-1]['Index']
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:
                    
                    while files:
                        try:
                            obj = files.pop(0)
                            page = obj['Page']
                            idx = obj['Index']
                            object_path, size = obj['Key'], obj['Size']
                            name = obj['SharedName']
                            shm = shared_memory.SharedMemory(name=name)
                            zip_file.writestr(object_path, shm.buf[:size].tobytes(), compress_type = zipfile.ZIP_BZIP2)
                            shm.close()
                            shm.unlink()
                        except:
                            pass
                    zip_file.close()

                zip_buffer.seek(0)
                buffer_size = zip_buffer.getbuffer().nbytes
                destination_name = os.path.join(self.destination_directory, save_name)
                with open(destination_name, 'wb') as fd:
                    fd.write(zip_buffer.getvalue())
                self.logger(f'{last_page} {last_idx} ' + f'Downloaded: {destination_name} Size: {buffer_size} ' + f'Total: {self.obj_size}')

                time.sleep(self.timeout) if self.timeout else None
        else:
            print('Error source bucket and destination directory are needed to download')
            
    def local_zip(self, files = [], save_name = '', error_log = ''):
        if all((self.source_directory, self.destination_directory)):
            zip_buffer = io.BytesIO()

            with SharedMemoryManager() as smm:
                for file_object in files:
                    object_size = file_object['Size']
                    shm_name = smm.SharedMemory(size=object_size).name
                    file_object['SharedName'] = shm_name
                    
                pool = multiprocessing.Pool(processes=self.njobs)
                pool_map = pool.map(get_file, files)
                pool.close()
                pool.join()
                
                if os.path.getsize(self.log_file) > (self.zip_size * 3):
                    os.rename(self.log_file, '.' + self.log_file.split('.')[1] + f'-{self.log_count}' + '.log')
                    with open(self.log_file, 'w'):
                        pass
                    self.logger('BZ2DATA Harvard Business School (2025)\n')
                    
                last_page, last_idx = files[-1]['Page'], files[-1]['Index']
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:

                    while files:
                        try:
                            obj = files.pop(0)
                            page = obj['Page']
                            idx = obj['Index']
                            object_path, size = obj['Key'], obj['Size']
                            name = obj['SharedName']
                            shm = shared_memory.SharedMemory(name=name)
                            zip_file.writestr(object_path, shm.buf[:size].tobytes(), compress_type = zipfile.ZIP_BZIP2)
                            shm.close()
                            shm.unlink()
                        except:
                            pass
                    zip_file.close()

                zip_buffer.seek(0)
                buffer_size = zip_buffer.getbuffer().nbytes
                destination_name = os.path.join(self.destination_directory, save_name)
                with open(destination_name, 'wb') as fd:
                    fd.write(zip_buffer.getvalue())
                self.logger(f'{last_page} {last_idx} ' + f'Compressed: {destination_name} Size: {buffer_size} ' + f'Total: {self.obj_size}')

                time.sleep(self.timeout) if self.timeout else None
        else:
            print('Error source directory and destination directory are needed to compress locally')

    def compress(self, action, resume = '', inventory = ''):

        with open(self.log_file, 'w'):
            pass
        self.logger('BZ2DATA Harvard Business School (2025)\n')

        source_t = (self.source_bucket, self.source_directory)
        destination_t = (self.destination_bucket, self.destination_directory)
        
        if any(source_t) and any(destination_t):
            
            aws_dir =  os.path.join(os.environ['HOME'], '.aws')
            os.makedirs(aws_dir, mode=0o700, exist_ok=True)
            creds_file = os.path.join(aws_dir, 'credentials')
            conf_file = os.path.join(aws_dir, 'config')
            
            if all((self.source_bucket, self.destination_bucket)):
                with open(creds_file, 'w') as creds_fd:
                    creds_fd.write(f'[default]\naws_access_key_id={self.src_key_id}\naws_secret_access_key={self.src_key}\n\n[destination]\naws_access_key_id={self.dest_key_id}\naws_secret_access_key={self.dest_key}')

                with open(conf_file, 'w') as conf_fd:
                    conf_fd.write(f'[default]\nregion={self.src_region}\noutput=json\n\n[profile destination]\nregion={self.dest_region}\noutput=text\n')

            if resume:
                df = pd.read_csv(resume, sep="\s+", header=None, usecols=[0, 1, 7, 8, 9, 10, 12, 14], names=['DATE', 'TIMESTAPM', 'PAGE', 'ID', 'ACTION', 'FILE', 'SIZE', 'TOTAL'], skiprows=[0, 1])
                resume_idx = df.where(df['ACTION'] != 'Adding:').last_valid_index()
                self.object_count = int(df.loc[resume_idx].FILE.split('.')[0][-1]) + 1
                Page, Id = df[['PAGE', 'ID']].iloc[resume_idx].values
                RIDX = df.loc[Id].ID

            match action:
                case 'upload':
                    zip_function = self.upload_zip
                    all_files = iglob(f'{self.source_directory}/**/*.*', recursive=True)
                    all_pages = paginate_generator(all_files, self.page_size)
                case 'local':
                    zip_function = self.local_zip
                    all_files = iglob(f'{self.source_directory}/**/*.*', recursive=True)
                    all_pages = paginate_generator(all_files, self.page_size)
                case 'download':
                    zip_function = self.download_zip
                    if inventory:
                        all_files = pd.read_csv(inventory, header=None, names=['Bucket', 'Key', 'Size'], low_memory=False)
                        all_pages = range(0, len(all_files), self.page_size)
                    else:
                        source_client = self.source.client('s3')
                        paginator = source_client.get_paginator('list_objects_v2')
                        all_pages = paginator.paginate(Bucket=self.source_bucket)
                case 'compress':
                    zip_function = self.generate_zip
                    if inventory:
                        all_files = pd.read_csv(inventory, header=None, names=['Bucket', 'Key', 'Size'], low_memory=False)
                        all_pages = range(0, len(all_files), self.page_size)
                    else:
                        source_client = self.source.client('s3')
                        paginator = source_client.get_paginator('list_objects_v2')
                        all_pages = paginator.paginate(Bucket=self.source_bucket)

            for pidx, page in enumerate(all_pages):

                if resume and pidx < Page:
                    continue
                    
                match action:
                    case 'upload':
                        page_list = page
                    case 'local':
                        page_list = page
                    case _:
                        if inventory:
                            page_list = all_files[page: page + self.page_size].iterrows()
                        else:
                            page_list = page.get('Contents', [])
                        
                for idx, obj in enumerate(page_list):

                    if resume and idx <= RIDX:
                        continue

                    match action:
                        case 'upload':
                            self.file_size = os.path.getsize(obj)
                            obj = {'Key': obj, 'Size': self.file_size, 'Index': idx, 'Page': pidx, 'ErrorLog': self.error_log}
                        case 'local':
                            self.file_size = os.path.getsize(obj)
                            obj = {'Key': obj, 'Size': self.file_size, 'Index': idx, 'Page': pidx, 'ErrorLog': self.error_log}
                        case _:
                            if inventory:
                                self.file_size = obj[1]['Size']
                                obj = {'Key': obj[1]['Key'], 'Size': obj[1]['Size'], 'Index': idx, 'Page': pidx, 'KeyId': self.src_key_id, 'BucketKey': self.src_key, 'Bucket': self.source_bucket, 'ErrorLog': self.error_log}
                            else:
                                self.file_size = obj['Size']
                                obj = {'Key': obj['Key'], 'Size': obj['Size'], 'Index': idx, 'Page': pidx, 'KeyId': self.src_key_id, 'BucketKey': self.src_key, 'Bucket': self.source_bucket, 'ErrorLog': self.error_log}

                    object_path = obj['Key']
                    
                    if self.file_size == 0:
                        with open(self.error_log, 'a') as error_file:
                            error_file.write(f'Error file {object_path} size is zero bytes\n')
                        continue
                    
                    if self.file_size > self.max_file_size:

                        match action:
                            case 'upload':
                                try:
                                    destination_client = self.destination.client('s3')
                                    self.logger(f'{pidx} {idx} ' + f'Adding: {object_path} Size: {self.file_size} ' + f'Total: {self.file_size}')
                                    destination_client.upload_file(obj['Key'], self.destination_bucket, obj['Key'], ExtraArgs = { 'StorageClass' : self.destination_class }, Config=self.config)
                                    self.logger(f'{pidx} {idx} ' + f'Uploaded: {object_path} Size: {self.file_size} ' + f'Total: {self.file_size}')
                                    time.sleep(self.timeout) if self.timeout else None
                                except:
                                    with open(self.error_log, 'a') as error_file:
                                        error_file.write(f'Error uploading {object_path}\n')
                            case 'local':
                                try:
                                    self.logger(f'{pidx} {idx} ' + f'Adding: {object_path} Size: {self.file_size} ' + f'Total: {self.file_size}')
                                    shutil.copyfile(object_path, self.destination_directory)
                                    self.logger(f'{pidx} {idx} ' + f'Uploaded: {object_path} Size: {self.file_size} ' + f'Total: {self.file_size}')
                                    time.sleep(self.timeout) if self.timeout else None
                                except:
                                    with open(self.error_log, 'a') as error_file:
                                        error_file.write(f'Error compressing {object_path}\n')
                            case 'download':
                                try:
                                    self.logger(f'{pidx} {idx} ' + f'Adding: {object_path} Size: {self.file_size} ' + f'Total: {self.file_size}')
                                    destination_name = os.path.join(self.destination_directory, obj['Key'])
                                    source_client.download_file(self.destination_bucket, obj['Key'], destination_name, Config=self.config)
                                    self.logger(f'{pidx} {idx} ' + f'Downloaded: {object_path} Size: {self.file_size} ' + f'Total: {self.file_size}')
                                    time.sleep(self.timeout) if self.timeout else None
                                except:
                                    with open(self.error_log, 'a') as error_file:
                                        error_file.write(f'Error downloading {object_path}\n')
                            case 'compress':
                                try:
                                    botocore_config = botocore.config.Config(max_pool_connections = self.njobs, total_max_attempts = 5)
                                    source_client = boto3.client('s3', config = botocore_config)

                                    transfer_config = s3transfer.TransferConfig(
                                        use_threads=True,
                                        max_concurrency=self.njobs,
                                    )
                                    s3t = s3transfer.create_transfer_manager(source_client, transfer_config)
                                    copy_source = {
                                        'Bucket': self.source_bucket,
                                        'Key': obj['Key']
                                    }
                                    self.logger(f'{pidx} {idx} ' + f'Adding: {object_path} Size: {self.file_size} ' + f'Total: {self.file_size}')
                                    s3t.copy(copy_source=copy_source,
                                             bucket = self.destination_bucket,
                                             key = obj['Key'], extra_args = { 'StorageClass' : self.destination_class })

                                    # close transfer job
                                    s3t.shutdown()
                                    self.logger(f'{pidx} {idx} ' + f'Uploaded: {object_path} Size: {self.file_size} ' + f'Total: {self.file_size}')
                                    time.sleep(self.timeout) if self.timeout else None
                                except:
                                    with open(self.error_log, 'a') as error_file:
                                        error_file.write(f'Error transferring {object_path}\n')
                        continue

                    self.obj_size += self.file_size
                    # Zip buffer size has been exceeded
                    if self.obj_size > self.zip_size:

                        # When the buffer is bigger than the zip size, zip and upload the buffer then continue adding files to a new zip buffer, the opposite
                        # when buffer is too small and the latest object added was too big to prevent sporadic tiny zip files in the dataset.
                        if self.file_size >= self.zip_size or (self.obj_size - self.file_size) < self.file_size:

                            destination_key = self.zip_name + '-' + str(self.object_count) + '.zip'
                            self.obj_size -= self.file_size
                            zip_function([obj], destination_key, error_log = self.error_log)
                            self.object_count += 1
                            continue

                        destination_key = self.zip_name + '-' + str(self.object_count) + '.zip'
                        self.obj_size -= self.file_size
                        zip_function(self.zip_list, destination_key, error_log = self.error_log)
                        self.object_count += 1
                        self.obj_size = self.file_size
                    
                    self.logger(f'{pidx} {idx} ' + f'Adding: {object_path} Size: {self.file_size} ' + f'Total: {self.obj_size}')
                    self.zip_list.append(obj)
                
            if len(self.zip_list) != 0:
                destination_key = self.zip_name + '-' + str(self.object_count) + '.zip'
                zip_function(self.zip_list, destination_key, error_log = self.error_log)
                self.object_count += 1
                self.obj_size = 0

        else:
            self.logger('Source and Destination bucket or path required')
