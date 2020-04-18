#!/usr/bin/python3

import gcp_tools as gcp

def mainline():

    pub_client = gcp.create_publisher()
    topic = gcp.get_topic_path(pub_client, 'your-project', 'your-topic')

    for x in range(1,11):
        
        msg = 'Message #: ' + str(x)
        
        pub = gcp.publish(pub_client, topic, msg)
        
        if pub.reply == 'Y':
            print('Success! Published: ' + '"' + msg + '". ' + 'Here is the Message ID: ' + pub.detail)
        else:
            print('Failure! ' + pub.detail)

mainline()


