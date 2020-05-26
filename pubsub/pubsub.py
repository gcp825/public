#!/usr/bin/python3

from gcp_tools import io_tools as io

def run():
    
#   find and replace: your-project
#   find and replace: your-topic
#   find and replace: your-subscription

#   publish messages (printing out publish status to the console)

    pub_client = io.create_publisher()
    topic = io.get_topic_path(pub_client, 'your-project', 'your-topic')

    for x in range(1,11):
        
        msg = 'Message #: ' + str(x)
        
        pub = io.publish(pub_client, topic, msg)
        
        if pub.reply == 'Y':
            print('Success! Published: ' + '"' + msg + '". ' + 'Here is the Message ID: ' + pub.detail)
        else:
            print('Failure! ' + pub.detail)
            
#   retrieve messages (printing out to the console)

    sub_client = io.create_subscriber()
    subscription = io.get_subscription_path(sub_client, 'your-project', 'your-subscription')

    returned_messages = io.async_pull(sub_client, subscription)

    messages = []
    
    for x in returned_messages: messages.append(x.data.decode('utf-8'))
    
    sorted_messages = sorted(sorted(messages),key=len)
    
    for x in sorted_messages: print(x)
    
if __name__ == '__main__': run()

