
from filelog import get_logger
from glob import glob
import zipfile
import boto3
import time
import io
import os

class DataManager():

    def __init__(self, zip_size = 5000000000, archive_names = 'bz2data-zip-archive', destination_class = 'STANDARD', verbose_level = '', log_file = './bz2data.log', timeout = 0):

        '''

        S3 storage classes:
        'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'|'SNOW'|'EXPRESS_ONEZONE'

        '''

        self.verbose_level = verbose_level
        self.logger = get_logger(level = self.verbose_level, log_file = log_file) if self.verbose_level else False
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
        self.page_size = ''
        self.destination_directory = ''
        
        self.logger('BZ2DATA Harvard Business School (2025)\n') if self.verbose_level else None

    def sourceBucket(self, key_id = '', key = '', bucket = ''):
        if all((key_id, key, bucket)):
            source_sess = boto3.Session(
                aws_access_key_id = key_id,
                aws_secret_access_key = key
            )
            self.source = source_sess
            self.source_bucket = bucket
            s3source = source_sess.resource('s3')
            source_bucket = s3source.Bucket(bucket)
            
#           # Counting the objects can take a very long time for large datasets and there is an additional charge for listing them as well.
#            total_objs = sum(1 for _ in source_bucket.objects.all())
#            self.objects = total_objs
#            total_bytes = sum([object.size for object in source_bucket.objects.all()])
#            self.logger(f'\nSource:\nTotal bucket size: {total_bytes/1024/1024/1024} GB\ntotal bucket objects: {total_objs}') if self.verbose_level else None

    def destinationBucket(self, key_id = '', key = '', bucket = ''):
        if all((key_id, key, bucket)):
            dest_sess = boto3.Session(
                aws_access_key_id = key_id,
                aws_secret_access_key = key
            )
            self.destination = dest_sess
            self.destination_bucket = bucket
            s3destination = dest_sess.resource('s3')
            destination_bucket = s3destination.Bucket(bucket)
            
    def sourcePath(self, source_directory = '', page_size = 1000):
        self.source_directory = source_directory
        self.page_size = page_size
        if os.path.exists(self.source_directory):
            self.logger(f'The path {self.source_directory} exists.') if self.verbose_level else None
        else:
            self.logger(f'The path {self.source_directory} does not exist.') if self.verbose_level else None
            
    def destinationPath(self, destination_directory = ''):
        self.destination_directory = destination_directory
        if os.path.exists(self.destination_directory):
            self.logger(f'The path {self.source_directory} exists.') if self.verbose_level else None
        else:
            self.logger(f'The path {self.source_directory} does not exist.') if self.verbose_level else None

    def generate_zip(self, files = [], save_name = ''):
        source_client = self.source.client('s3')
        destination_client = self.destination.client('s3')
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:
            object_size = 0
            while files:
                object = files.pop(0)
                object_size += object['Size']
                file_obj = source_client.get_object(Bucket = self.source_bucket, Key = object['Key'])
                file_content = file_obj['Body'].read()
                self.logger(f'Writing: {object['Key']} Size: {object['Size']}' + ' ' + f'Total: {object_size}') if self.verbose_level else None
                zip_file.writestr(object['Key'], file_content, compress_type = zipfile.ZIP_BZIP2)
            zip_file.close()

        zip_buffer.seek(0)
        self.logger(f'Uploading {save_name}') if self.verbose_level else None
        t = time.time()
        destination_client.put_object(Bucket = self.destination_bucket, Key = save_name, Body = zip_buffer, StorageClass = self.destination_class)
        upload_t = time.time() - t
        self.logger(f'Upload time: {upload_t}\n') if self.verbose_level else None
        
        time.sleep(self.timeout) if self.timeout else None

    def upload_zip(self, files = [], save_name = ''):
        destination_client = self.destination.client('s3')
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:
            object_size = 0
            while files:
                object = files.pop(0)
                size = os.path.getsize(object)
                object_size += size
                file_descriptor = os.open(object, os.O_RDONLY)
                file_content = os.read(file_descriptor, size)
                os.close(file_descriptor)
                self.logger(f'Writing: {object} Size: {size}' + ' ' + f'Total: {object_size}') if self.verbose_level else None
                zip_file.writestr(object, file_content, compress_type = zipfile.ZIP_BZIP2)
            zip_file.close()

        zip_buffer.seek(0)
        self.logger(f'Uploading {save_name}') if self.verbose_level else None
        t = time.time()
        destination_client.put_object(Bucket = self.destination_bucket, Key = save_name, Body = zip_buffer, StorageClass = self.destination_class)
        upload_t = time.time() - t
        self.logger(f'Upload time: {upload_t}\n') if self.verbose_level else None
        
        time.sleep(self.timeout) if self.timeout else None

    def download_zip(self, files = [], save_name = ''):
        source_client = self.source.client('s3')
        zip_buffer = io.BytesIO()
        t = time.time()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_BZIP2) as zip_file:
            object_size = 0
            while files:
                object = files.pop(0)
                object_size += object['Size']
                file_obj = source_client.get_object(Bucket = self.source_bucket, Key = object['Key'])
                file_content = file_obj['Body'].read()
                self.logger(f'Writing: {object['Key']} Size: {object['Size']}' + ' ' + f'Total: {object_size}') if self.verbose_level else None
                zip_file.writestr(object['Key'], file_content, compress_type = zipfile.ZIP_BZIP2)
            zip_file.close()

        zip_buffer.seek(0)
        destination_name = os.path.join(self.destination_directory, save_name)
        self.logger(f'Downloading to {destination_name}') if self.verbose_level else None
        
        with open(destination_name, 'wb') as fd:
            fd.write(zip_buffer.getvalue())
        download_t = time.time() - t
        self.logger(f'Download time: {download_t}\n') if self.verbose_level else None
        
        time.sleep(self.timeout) if self.timeout else None
        
    def transfer(self):
        if all((self.source_bucket, self.destination_bucket)):
            source_client = self.source.client('s3')
            destination_client = self.destination.client('s3')
            paginator = source_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self.source_bucket):

                for idx, obj in enumerate(page.get('Contents', [])):

                    file_obj = source_client.get_object(Bucket=self.source_bucket, Key=obj['Key'])
                    file_content = file_obj['Body'].read()
                    destination_key = obj['Key']
                    self.logger(f'\nUploading {destination_key}') if self.verbose_level else None
                    destination_client.put_object(Bucket = self.destination_bucket, Key = destination_key, Body = file_content, StorageClass = self.destination_class)

    def compress(self):
        if all((self.source_bucket, self.destination_bucket)):
            source_client = self.source.client('s3')
            paginator = source_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self.source_bucket):

                for idx, obj in enumerate(page.get('Contents', [])):

                    self.source_count += 1
                    self.file_size = obj['Size']
                    self.obj_size += self.file_size

                    # Zip buffer size has been exceeded
                    
                    if self.obj_size > self.zip_size:
                        self.logger(f'Exceeded zip size: {self.obj_size}\n') if self.verbose_level else None

                        # When the file is bigger than the zip size, zip and upload the file then continue adding files to the zip buffer, same
                        # when the buffer is too small to prevent sporadic tiny zip files in the dataset.
                        
                        if self.file_size >= self.zip_size or (self.obj_size - self.file_size) < self.file_size:
                            self.logger(f'File exceeds zip size or zip buffer is too small\n') if self.verbose_level else None
                            buff_size = self.obj_size - self.file_size
                            self.logger(f'Object: {self.file_size} Buffer {buff_size}') if self.verbose_level else None

                            destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
                            self.generate_zip([obj], destination_key)
                            self.object_count += 1
                            self.obj_size = self.obj_size - self.file_size
                            continue

                        destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
                        self.generate_zip(self.zip_list, destination_key)
                        self.object_count += 1
                        self.obj_size = self.file_size

                    self.zip_list.append(obj)

            destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
            self.generate_zip(self.zip_list, destination_key)
            self.object_count += 1

    def upload(self):
        if all((self.source_directory, self.destination_bucket)):
            all_files = glob(f'{self.source_directory}/**/*.*', recursive=True)
            
            for page in range(0, len(all_files), self.page_size):
                page_list = all_files[page: page + self.page_size]
                
                for idx, obj in enumerate(page_list):
                    self.source_count += 1
                    self.file_size = os.path.getsize(obj)
                    self.obj_size += self.file_size

                    # Zip buffer size has been exceeded
                    
                    if self.obj_size > self.zip_size:
                        self.logger(f'Exceeded zip size: {self.obj_size}\n') if self.verbose_level else None

                        # When the file is bigger than the zip size, zip and upload the file then continue adding files to the zip buffer, same
                        # when the buffer is too small to prevent sporadic tiny zip files in the dataset.
                        
                        if self.file_size >= self.zip_size or (self.obj_size - self.file_size) < self.file_size:
                            self.logger(f'File exceeds zip size or zip buffer is too small\n') if self.verbose_level else None
                            buff_size = self.obj_size - self.file_size
                            self.logger(f'Object: {self.file_size} Buffer {buff_size}') if self.verbose_level else None

                            destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
                            self.upload_zip([obj], destination_key)
                            self.object_count += 1
                            self.obj_size = self.obj_size - self.file_size
                            continue

                        destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
                        self.upload_zip(self.zip_list, destination_key)
                        self.object_count += 1
                        self.obj_size = self.file_size

                    self.zip_list.append(obj)
                    
            destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
            self.upload_zip(self.zip_list, destination_key)
            self.object_count += 1

    def stage(self):
        if all((self.source_bucket, self.destination_directory)):
            source_client = self.source.client('s3')
            paginator = source_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self.source_bucket):

                for idx, obj in enumerate(page.get('Contents', [])):

                    self.source_count += 1
                    self.file_size = obj['Size']
                    self.obj_size += self.file_size

                    # Zip buffer size has been exceeded
                    
                    if self.obj_size > self.zip_size:
                        self.logger(f'Exceeded zip size: {self.obj_size}\n') if self.verbose_level else None

                        # When the file is bigger than the zip size, zip and upload the file then continue adding files to the zip buffer, same
                        # when the buffer is too small to prevent sporadic tiny zip files in the dataset.
                        
                        if self.file_size >= self.zip_size or (self.obj_size - self.file_size) < self.file_size:
                            self.logger(f'File exceeds zip size or zip buffer is too small\n') if self.verbose_level else None
                            buff_size = self.obj_size - self.file_size
                            self.logger(f'Object: {self.file_size} Buffer {buff_size}') if self.verbose_level else None

                            destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
                            self.download_zip([obj], destination_key)
                            self.object_count += 1
                            self.obj_size = self.obj_size - self.file_size
                            continue

                        destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
                        self.download_zip(self.zip_list, destination_key)
                        self.object_count += 1
                        self.obj_size = self.file_size

                    self.zip_list.append(obj)

            destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
            self.download_zip(self.zip_list, destination_key)
            self.object_count += 1

