#!/usr/bin/python3

import gcp_tools as gcp

def mainline():

    storage_client = gcp.create_storage_client()
    bucket = gcp.get_bucket_path(storage_client, 'your-bucket-name')

#   target path will be created if it does not exist
    gcp.file_download(bucket, '/source_path', 'source_file.extension', '~/target_path', 'target_file.extension')
    
#   if target file name not supplied, defaults to the same as source file name    
    gcp.file_upload(bucket, '~/source_path', 'source_file.extension', '/new')
    
mainline()
