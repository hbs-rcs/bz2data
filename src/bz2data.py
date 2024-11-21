
import zipfile
import boto3
import time
import io

class DataManager():

    def __init__(self, zip_size = 5000000000, archive_names = '', destination_class = 'STANDARD'):

        '''

        S3 storage classes:
        'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'|'SNOW'|'EXPRESS_ONEZONE'

        '''

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
            total_objs = sum(1 for _ in source_bucket.objects.all())
            self.objects = total_objs

            total_bytes = sum([object.size for object in source_bucket.objects.all()])
            print(f'\nSource:\nTotal bucket size: {total_bytes/1024/1024/1024} GB\ntotal bucket objects: {total_objs}')

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
            total_objs = sum(1 for _ in destination_bucket.objects.all())

            total_bytes = sum([object.size for object in destination_bucket.objects.all()])
            print(f'\nDestination:\nTotal bucket size: {total_bytes/1024/1024/1024} GB\ntotal bucket objects: {total_objs}')

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
                print(f'\nWriting: {object['Key']}\nSize: {object['Size']}' + ' ' + f'Total: {object_size}')
                zip_file.writestr(object['Key'], file_content, compress_type = zipfile.ZIP_BZIP2)
            zip_file.close()

        zip_buffer.seek(0)
        print(f'\nUpdloading {save_name}')
        destination_client.put_object(Bucket = self.destination_bucket, Key = save_name, Body = zip_buffer, StorageClass = self.destination_class)

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
                    print(f'\nUpdloading {destination_key}')
                    destination_client.put_object(Bucket = self.destination_bucket, Key = destination_key, Body = file_content, StorageClass = self.destination_class)

    def compress(self):
        if all((self.source_bucket, self.destination_bucket, self.zip_name)):
            source_client = self.source.client('s3')
            paginator = source_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self.source_bucket):

                for idx, obj in enumerate(page.get('Contents', [])):

                    self.source_count += 1
                    self.file_size = obj['Size']
                    self.obj_size += self.file_size

                    if self.obj_size > self.zip_size:
                        print(f'\nExceeded zip size: {self.obj_size}')

                        if self.file_size >= self.zip_size or (self.obj_size - self.file_size) < self.file_size:
                            print(f'\nFile exceeds zip size or zip buffer is too small')
                            buff_size = self.obj_size - self.file_size
                            print(f'Object: {self.file_size} Buffer {buff_size}')

                            if self.source_count == self.objects:
                                destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
                                self.generate_zip(self.zip_list, destination_key)
                                self.object_count += 1

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

                    if self.source_count == self.objects:
                        destination_key = f'{self.zip_name + '-' + str(self.object_count)}.bz2'
                        self.generate_zip(self.zip_list, destination_key)
                        self.object_count += 1


