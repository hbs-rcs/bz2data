
import zipfile
import boto3
import time
import io

class DataManager():

    def __init__(self, zip_size = 5000000000, destination_class = 'STANDARD', merge_dirs = True):
    
        '''
        
        S3 storage classes:
        'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'|'SNOW'|'EXPRESS_ONEZONE'
        
        '''
        
        self.zip_buffer = io.BytesIO()
        self.source = boto3.Session
        self.destination = boto3.Session
        self.destination_class = destination_class
        self.source_bucket = ''
        self.destination_bucket = ''
        self.object_count = 0
        self.obj_size = 0
        self.file_size = 0
        self.objects = 0
        self.zip_size = zip_size
        self.merge_dirs = merge_dirs
        self.same_dir = True
        self.source_count = 0

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

    def transfer(self):
        if all((self.source_bucket, self.destination_bucket)):
            source_client = self.source.client('s3')
            destination_client = self.destination.client('s3')
            paginator = source_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self.source_bucket):

                for idx, obj in enumerate(page.get('Contents', [])):

                    self.source_count += 1
                    self.file_size = obj['Size']
                    self.obj_size += self.file_size
                    previous = page.get('Contents', [])[idx - 1]['Key'].split('/')[0]
                    current = obj['Key'].split('/')[0]

                    if self.merge_dirs != True:
                        if idx == 0:
                            current = previous
                        self.same_dir = current == previous

                    if self.obj_size >= self.zip_size or self.same_dir == False:
                        print(f'\nExceeded zip size or different top level directory')
                        print(f'Zip size: {self.obj_size} Current Dir: {obj['Key'].split('/')[0]}')

                        if self.file_size >= self.zip_size or (self.obj_size - self.file_size) < self.file_size:
                            print(f'\nFile exceeds zip size or zip buffer')
                            buff_size = self.obj_size - self.file_size
                            print(f'Object: {self.file_size} Buffer {buff_size}')

                            if self.source_count == self.objects:
                                self.zip_buffer.seek(0)
                                destination_key = f'{previous + '-' + str(self.object_count)}.zip.bz2'
                                print(f'\nUpdloading {destination_key}')
                                destination_client.put_object(Bucket=self.destination_bucket, Key=destination_key, Body=self.zip_buffer, StorageClass = self.destination_class)
                                self.object_count += 1

                            zip_buffer = io.BytesIO()
                            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                                file_obj = source_client.get_object(Bucket=self.source_bucket, Key=obj['Key'])
                                file_content = file_obj['Body'].read()
                                zip_file.writestr(obj['Key'], file_content, compress_type=zipfile.ZIP_BZIP2)
                                zip_buffer.seek(0)
                                destination_key = f'{current + '-' + str(self.object_count)}.zip.bz2'
                                print(f'\nUpdloading {destination_key}')
                                destination_client.put_object(Bucket=self.destination_bucket, Key=destination_key, Body=zip_buffer, StorageClass = self.destination_class)
                                self.object_count += 1
                                self.obj_size = self.obj_size - self.file_size

                            continue

                        self.zip_buffer.seek(0)
                        destination_key = f'{previous + '-' + str(self.object_count)}.zip.bz2'
                        print(f'\nUpdloading {destination_key}')
                        destination_client.put_object(Bucket=self.destination_bucket, Key=destination_key, Body=self.zip_buffer, StorageClass = self.destination_class)
                        self.zip_buffer = io.BytesIO()
                        self.object_count += 1
                        self.obj_size = self.file_size

                    with zipfile.ZipFile(self.zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        print(f'\nWriting: {obj['Key']}\nSize: {self.file_size}' + ' ' + f'Total: {self.obj_size}')
                        file_obj = source_client.get_object(Bucket=self.source_bucket, Key=obj['Key'])
                        file_content = file_obj['Body'].read()
                        zip_file.writestr(obj['Key'], file_content, compress_type=zipfile.ZIP_BZIP2)

                    if self.source_count == self.objects:
                        self.zip_buffer.seek(0)
                        destination_key = f'{current + '-' + str(self.object_count)}.zip.bz2'
                        print(f'\nUpdloading {destination_key}')
                        destination_client.put_object(Bucket=self.destination_bucket, Key=destination_key, Body=self.zip_buffer, StorageClass = self.destination_class)
                        self.zip_buffer = io.BytesIO()

