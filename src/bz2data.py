
from multiprocessing import shared_memory
from filelog import get_logger
import multiprocessing
from glob import glob
import pandas as pd
import zipfile
import boto3
import time
import io
import os

def get_object(obj):

    page = obj['Page']
    idx = obj['Index']
    file_key = obj['Key']
    key_id = obj['KeyId']
    bucket_key = obj['BucketKey']
    bucket = obj['Bucket']
    error_log = obj['ErrorLog']
    name = f'{page}-{idx}'
    size = obj['Size']
    objshm = shared_memory.SharedMemory(create = True, name = name, size=size)

    source_sess = boto3.Session(aws_access_key_id = key_id, aws_secret_access_key = bucket_key)
    source_client = source_sess.client('s3')

    try:
        with open(f'./{bucket}-progress.log', 'a') as fd:
            fd.write(f'Downloading {file_key}\n')
        key_obj = source_client.get_object(Bucket = bucket, Key = file_key)
        objshm.buf[:size] = key_obj['Body'].read()
    except Exception as e:
        objshm.unlink()

        with open(error_log, 'a') as error_file:
            loggerr = get_logger(level = 'ERROR', log_file = error_log)
            loggerr(f'Error downloading {file_key}', e)

def get_file(obj):

    page = obj['Page']
    idx = obj['Index']
    file_key = obj['Key']
    error_log = obj['ErrorLog']
    size = obj['Size']
    name = f'{page}-{idx}'
    objshm = shared_memory.SharedMemory(create = True, name = name, size=size)


    file_descriptor = os.open(file_key, os.O_RDONLY)

    try:
        objshm.buf[:size] = os.read(file_descriptor, size)
        os.close(file_descriptor)
    except Exception as e:
        objshm.unlink()

        with open(error_log, 'a') as error_file:
            loggerr = get_logger(level = 'ERROR', log_file = error_log)
            loggerr(f'Error downloading {file_key}', e)
            
class DataManager():

    def __init__(self, zip_size = 5000000000, archive_names = 'bz2data-zip-archive', destination_class = 'STANDARD', njobs = 1, log_file = './bz2data.log', error_log = './bz2data-error.log', timeout = 0):

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
        self.source_count = 0
        self.zip_list = []
        self.timeout = timeout
        self.source_directory = ''
        self.page_size = 1000
        self.destination_directory = ''
        self.njobs = njobs
        self.key_id = ''
        self.key = ''
        self.pool = multiprocessing.Pool(processes=self.njobs)

    def sourceBucket(self, key_id = '', key = '', bucket = ''):
        if all((key_id, key, bucket)):
            self.key_id = key_id
            self.key = key
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

    def destinationBucket(self, key_id = '', key = '', bucket = ''):
        if all((key_id, key, bucket)):
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

            pool_map = self.pool.map(get_object, files)
            
            if os.path.getsize(self.log_file) > (self.zip_size * 3):
                os.rename(self.log_file, '.' + self.log_file.split('.')[1] + f'-{self.log_count}' + '.log')
                with open(self.log_file, 'w'):
                    pass
                self.logger('BZ2DATA Harvard Business School (2025)\n')
                
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:
                
                while files:
                    try:
                        obj = files.pop(0)
                        page = obj['Page']
                        idx = obj['Index']
                        object_path, size = obj['Key'], obj['Size']
                        name = f'{page}-{idx}'
                        shm = shared_memory.SharedMemory(name=name)
                        self.logger(f'{page} {idx} ' + f'Adding: {object_path} Size: {size} ' + f'Total: {self.obj_size}')
                        zip_file.writestr(object_path, shm.buf[:size].tobytes(), compress_type = zipfile.ZIP_BZIP2)
                        shm.close()
                        shm.unlink()
                    except:
                        pass
                zip_file.close()

            zip_buffer.seek(0)
            buffer_size = zip_buffer.getbuffer().nbytes
            destination_client.put_object(Bucket = self.destination_bucket, Key = save_name, Body = zip_buffer, StorageClass = self.destination_class)
            self.logger(f'{page} {idx} ' + f'Uploaded: {save_name} Size: {buffer_size} ' + f'Total: {self.obj_size}')

            time.sleep(self.timeout) if self.timeout else None
        else:
            print('Error source bucket and destination bucket are needed to compress')

    def upload_zip(self, files = [], save_name = '', error_log = ''):
        if all((self.source_directory, self.destination_bucket)):
            destination_client = self.destination.client('s3')
            zip_buffer = io.BytesIO()

            pool_map = self.pool.map(get_file, files)
            
            if os.path.getsize(self.log_file) > (self.zip_size * 3):
                os.rename(self.log_file, '.' + self.log_file.split('.')[1] + f'-{self.log_count}' + '.log')
                with open(self.log_file, 'w'):
                    pass
                self.logger('BZ2DATA Harvard Business School (2025)\n')
                
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:

                while files:
                    try:
                        obj = files.pop(0)
                        page = obj['Page']
                        idx = obj['Index']
                        object_path, size = obj['Key'], obj['Size']
                        name = f'{page}-{idx}'
                        shm = shared_memory.SharedMemory(name=name)
                        self.logger(f'{page} {idx} ' + f'Adding: {object_path} Size: {size} ' + f'Total: {self.obj_size}')
                        zip_file.writestr(object_path, shm.buf[:size].tobytes(), compress_type = zipfile.ZIP_BZIP2)
                        shm.close()
                        shm.unlink()
                    except:
                        pass
                zip_file.close()

            zip_buffer.seek(0)
            buffer_size = zip_buffer.getbuffer().nbytes
            destination_client.put_object(Bucket = self.destination_bucket, Key = save_name, Body = zip_buffer, StorageClass = self.destination_class)
            self.logger(f'{page} {idx} ' + f'Uploaded: {save_name} Size: {buffer_size} ' + f'Total: {self.obj_size}')

            time.sleep(self.timeout) if self.timeout else None
        else:
            print('Error source directory and destination bucket are needed to upload')

    def download_zip(self, files = [], save_name = '', error_log = ''):
        if all((self.source_bucket, self.destination_directory)):
            zip_buffer = io.BytesIO()

            pool_map = self.pool.map(get_object, files)

            if os.path.getsize(self.log_file) > (self.zip_size * 3):
                os.rename(self.log_file, '.' + self.log_file.split('.')[1] + f'-{self.log_count}' + '.log')
                with open(self.log_file, 'w'):
                    pass
                self.logger('BZ2DATA Harvard Business School (2025)\n')

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:
                
                while files:
                    try:
                        obj = files.pop(0)
                        page = obj['Page']
                        idx = obj['Index']
                        object_path, size = obj['Key'], obj['Size']
                        name = f'{page}-{idx}'
                        shm = shared_memory.SharedMemory(name=name)
                        self.logger(f'{page} {idx} ' + f'Adding: {object_path} Size: {size} ' + f'Total: {self.obj_size}')
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
            self.logger(f'{page} {idx} ' + f'Downloaded: {destination_name} Size: {buffer_size} ' + f'Total: {self.obj_size}')

            time.sleep(self.timeout) if self.timeout else None
        else:
            print('Error source bucket and destination directory are needed to download')

    def transfer(self):
        if all((self.source_bucket, self.destination_bucket)):
            source_client = self.source.client('s3')
            destination_client = self.destination.client('s3')
            paginator = source_client.get_paginator('list_objects_v2')

            for pidx, page in enumerate(paginator.paginate(Bucket=self.source_bucket)):

                for idx, obj in enumerate(page.get('Contents', [])):

                    file_obj = source_client.get_object(Bucket=self.source_bucket, Key=obj['Key'])
                    file_content = file_obj['Body'].read()
                    self.file_size = obj['Size']
                    self.obj_size += self.file_size
                    destination_key = obj['Key']
                    destination_client.put_object(Bucket = self.destination_bucket, Key = destination_key, Body = file_content, StorageClass = self.destination_class)
                    self.logger(f'{pidx} {idx} ' + f'Listed: {destination_key} Size: {self.file_size} ' + f'Total: {self.obj_size}')

    def getList(self):
        if self.source_bucket:

            # Clear log file
            with open(self.log_file, 'w'):
                pass
            self.logger('BZ2DATA Harvard Business School (2025)\n')

            source_client = self.source.client('s3')
            paginator = source_client.get_paginator('list_objects_v2')

            for pidx, page in enumerate(paginator.paginate(Bucket=self.source_bucket)):

                for idx, obj in enumerate(page.get('Contents', [])):
                
                    self.source_count += 1
                    self.file_size = obj['Size']
                    self.obj_size += self.file_size
                    destination_key = obj['Key']

                    self.logger(f'{pidx} {idx} ' + f'Listed: {destination_key} Size: {self.file_size} ' + f'Total: {self.obj_size}')

    def compress(self, action, resume = '', inventory = ''):

        with open(self.log_file, 'w'):
            pass
        self.logger('BZ2DATA Harvard Business School (2025)\n')
        
        if resume:
            df = pd.read_csv(resume, sep="\s+", header=None, usecols=[0, 1, 7, 8, 9, 10, 12, 14], names=['DATE', 'TIMESTAPM', 'PAGE', 'ID', 'ACTION', 'FILE', 'SIZE', 'TOTAL'], skiprows=[0, 1])
            resume_idx = df.where(df['ACTION'] != 'Adding:').last_valid_index()
            self.object_count = int(df.loc[resume_idx].FILE.split('.')[0][-1]) + 1
            Page, Id = df[['PAGE', 'ID']].iloc[resume_idx].values
            RIDX = df.loc[Id].ID

        source_t = (self.source_bucket, self.source_directory)
        destination_t = (self.destination_bucket, self.destination_directory)
        
        if any(source_t) and any(destination_t):
        
            match action:
                case 'upload':
                    zip_function = self.upload_zip
                    all_files = glob(f'{self.source_directory}/**/*.*', recursive=True)
                    all_pages = range(0, len(all_files), self.page_size)
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
                        page_list = all_files[page: page + self.page_size]
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
                        case _:
                            if inventory:
                                self.file_size = obj[1]['Size']
                                obj = {'Key': obj[1]['Key'], 'Size': obj[1]['Size'], 'Index': idx, 'Page': pidx, 'KeyId': self.key_id, 'BucketKey': self.key, 'Bucket': self.source_bucket, 'ErrorLog': self.error_log}
                            else:
                                self.file_size = obj['Size']
                                obj = {'Key': obj['Key'], 'Size': obj['Size'], 'Index': idx, 'Page': pidx, 'KeyId': self.key_id, 'BucketKey': self.key, 'Bucket': self.source_bucket, 'ErrorLog': self.error_log}

                    self.source_count += 1
                    self.obj_size += self.file_size

                    # Zip buffer size has been exceeded
                    if self.obj_size > self.zip_size:

                        # When the buffer is bigger than the zip size, zip and upload the buffer then continue adding files to a new zip buffer, the opposite
                        # when buffer is too small and the latest object added was too big to prevent sporadic tiny zip files in the dataset.
                        if self.file_size >= self.zip_size or (self.obj_size - self.file_size) < self.file_size:

                            destination_key = self.zip_name + '-' + str(self.object_count) + '.zip'
                            self.obj_size -= self.file_size
                            error_log = self.error_log
                            zip_function([obj], destination_key, error_log = error_log)
                            self.object_count += 1
                            continue

                        destination_key = self.zip_name + '-' + str(self.object_count) + '.zip'
                        self.obj_size -= self.file_size
                        error_log = self.error_log
                        zip_function(self.zip_list, destination_key, error_log = error_log)
                        self.object_count += 1
                        self.obj_size = self.file_size

                    self.zip_list.append(obj)

            destination_key = self.zip_name + '-' + str(self.object_count) + '.zip'
            error_log = self.error_log
            zip_function(self.zip_list, destination_key, error_log = error_log)
            self.object_count += 1
            self.obj_size = 0
            
            self.pool.close()
            self.pool.join()
        else:
            self.logger('Source and Destination bucket or path required')
