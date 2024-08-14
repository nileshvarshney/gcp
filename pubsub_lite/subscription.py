from concurrent.futures._base import TimeoutError
from google.cloud.pubsublite.cloudpubsub import SubscriberClient
from google.cloud.pubsublite.types import (
    CloudRegion, 
    CloudZone,
    FlowControlSettings,
    SubscriptionPath
)

project_number = "294195642328"
cloud_region = "us-central1"
zone_id = "a"
subscription_id = "my-lite-subscription"
timeout = 50


location = CloudZone(CloudRegion(cloud_region), zone_id)
print(f"Location {location}")
subscription_path = SubscriptionPath(project_number, location, subscription_id)
print(f"Subscription Path {subscription_path}")

per_partition_flow_control_settings = FlowControlSettings(
     messages_outstanding=1000,
    bytes_outstanding = 10 * 1024 * 1024
)

def callback(message):
    message_data = message.data.decode("utf-8")
    print(f"Received {message_data} of ordering key {message.ordering_key}.")
    message.ack()

with SubscriberClient() as subscriber_client:
    streaming_pull_future = subscriber_client.subscribe(
        subscription_path, 
        callback = callback,
        per_partition_flow_control_settings=per_partition_flow_control_settings
    )
    print(f"Listening for message on {str(subscription_path)}...")
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError or KeyboardInterrupt:
        streaming_pull_future.cancel()
        assert streaming_pull_future.done()
