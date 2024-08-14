from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    MessageMetadata,
    TopicPath
)
import random

project_number = "294195642328"
cloud_region = "us-central1"
zone_id = "a"
topic_id = "my-lite-topic"
num_message = 100

location = CloudZone(CloudRegion(cloud_region), zone_id)
print(f"Location {location}")
topic_path = TopicPath(project_number, location, topic_id)
print(f"Topic Path {topic_path}")

with PublisherClient() as publisher_client:
    data = "Hello World " + str(random.randint(1, 1000))
    api_future = publisher_client.publish(topic_path, data.encode("utf-8"))
    message_id = api_future.result()
    publish_metadata = MessageMetadata.decode(message_id)
    print(f"Published a message to partition {publish_metadata.partition.value} and offset {publish_metadata.cursor.offset}")