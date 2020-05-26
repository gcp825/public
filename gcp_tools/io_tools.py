# Abstraction Layer for GCP functionality
#   Obscures some of the stranger GCP implementation decisions
#   Standardises approach to executing common functions

from collections  import namedtuple
from google.cloud import pubsub_v1, storage
import os

################################################################
# Storage: Download from Bucket add a comment
################################################################

def create_storage_client():

    return storage.Client()

def get_bucket_path(storage_client, bucket):
    
    return storage_client.bucket(bucket)

def file_upload(bucket_path, source_path, source_file, target_path, target_file=''):

    if source_path[0:2] == '/~': source_path = source_path[1:]                             # remove erroneous start slash if using ~ syntax
    if source_path[0:1] == '~': source_path = os.path.expanduser(source_path)              # switch ~ syntax for explicit source path
    if len(source_path) > 0 and source_path[0:1] != '/': source_path = '/' + source_path   # add missing start slash to source path
    if len(source_path) > 0 and source_path[-1:] != '/': source_path = source_path + '/'   # add missing end slash to source path
    
    if target_path[0:1] == '/': target_path = target_path[1:]                              # remove erroneous start slash from target path
    if len(target_path) > 0 and target_path[-1:] != '/': target_path = target_path + '/'   # add missing end slash to target path
    
    if len(target_file) == 0: target_file = source_file
     
    tgt_file = bucket_path.blob(target_path + target_file)
    tgt_file.upload_from_filename(source_path + source_file) 

def file_download(bucket_path, source_path, source_file, target_path, target_file=''):
    
    if source_path[0:1] == '/': source_path = source_path[1:]                              # remove erroneous start slash from source path
    if len(source_path) > 0 and source_path[-1:] != '/': source_path = source_path + '/'   # add missing end slash to source path

    if target_path[0:2] == '/~': target_path = target_path[1:]                             # remove erroneous start slash if using ~ syntax
    if target_path[0:1] == '~': target_path = os.path.expanduser(target_path)              # switch ~ syntax for explicit target path
    if len(target_path) > 0 and target_path[0:1] != '/': target_path = '/' + target_path   # add missing start slash to target path
    if len(target_path) > 0 and target_path[-1:] != '/': target_path = target_path + '/'   # add missing end slash to target path
    
    if len(target_file) == 0: target_file = source_file
     
    if not os.path.exists(target_path): os.makedirs(target_path)

    src_file = bucket_path.blob(source_path + source_file)
    src_file.download_to_filename(target_path + target_file)   

################################################################
# Pubsub: Publish
################################################################

def create_publisher():

    return pubsub_v1.PublisherClient()

def get_topic_path(publisher, project_id, topic):

    return publisher.topic_path(project_id, topic)

def publish(publisher, topic_path, msg):
    
    def callback(dummy):
#    pylint: disable=unused-argument
        pass

    bytestr = msg.encode('utf-8')                    # encode to a bytestring
    outcome = publisher.publish(topic_path,bytestr)  # publish the bytestring (returns a 'future' object)
    outcome.add_done_callback(callback)              # wait for completion of the 'future' i.e completion of the publish cmd
    
    if outcome.exception(timeout=30): 
        reply = 'N'
        detail = 'Publish to {} produced the following exception: {}.'.format(topic_name, outcome.exception())
    else:
        reply = 'Y'
        detail = outcome.result()

    Named_Tuple = namedtuple('pub_result','reply detail')
    return Named_Tuple(*(reply, detail))

################################################################
# Pubsub: Pull (Subscribe)
################################################################

def create_subscriber():

    return pubsub_v1.SubscriberClient()

def get_subscription_path(subscriber, project_id, subscription):

    return subscriber.subscription_path(project_id, subscription)

def async_pull(subscriber, subscription_path, timeout_secs=5):

    def callback(message):
        messages.append(message)
        message.ack()

    messages = []
    try_flag = True
    
    pull = subscriber.subscribe(subscription_path, callback)
    
    while try_flag is True:
        try:
            pull.result(timeout=timeout_secs)
        except:
            pull.cancel()
            try_flag = False
            
    return messages

def sync_pull(subscriber, subscription_path, limit=1000):

    pull_flag = False
    try:
        msg_limit = int(limit)
        if msg_limit > 0:
            pull_flag = True
            msg_limit = min(msg_limit,1000)
    except:
        pass
        
    if pull_flag is True:
        
        Named_Tuple = namedtuple('sub_msg','ack_id msg')
    
        response = subscriber.pull(subscription_path, max_messages=msg_limit, timeout=30)

        messages = []
        for x in response.received_messages:
            messages.append(Named_Tuple(*(x.ack_id, x.message.data)))
    
    else:
        
        raise Exception('Message Limit supplied to GCP Pull is not a positive integer')
    
    return messages        
