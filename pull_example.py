#!/usr/bin/python3

import gcp_tools as gcp

def mainline():

    sub_client = gcp.create_subscriber()
    subscription = gcp.get_subscription_path(sub_client, 'your-project', 'your-subscription')

    returned_messages = gcp.async_pull(sub_client, subscription)

    messages = []
    for x in returned_messages: messages.append(x.data.decode('utf-8'))
    
    sorted_messages = sorted(sorted(messages),key=len)
    for x in sorted_messages: print(x)
    
mainline()
