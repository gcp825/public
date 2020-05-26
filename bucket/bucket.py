#!/usr/bin/python3

from gcp_tools import io_tools as io

def run():
    
#   find and replace: your-bucket-name

#   if target file name not supplied, defaults to the same as source file name
#   target path will be created if it does not exist

    storage_client = io.create_storage_client()
    bucket = io.get_bucket_path(storage_client, 'your-bucket-name')

#   io.file_download(bucket, 'source_path', 'source_file.ext', 'target_path', 'target_file.ext')
    io.file_download(bucket, '/download', 'example.txt', '~/files', 'example.new')
    
#   io.file_upload(bucket, 'source_path', 'source_file.ext', 'target_path', 'target_file.ext')
    io.file_upload(bucket, '~/files', 'example.new', '/upload')
    
if __name__ == '__main__': run()

