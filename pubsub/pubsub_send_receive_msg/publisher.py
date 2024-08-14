from google.cloud import pubsub_v1
from faker import Faker

project_id = "playground-s-11-aba4c926"
topic_id = "funTopic"

publisher = pubsub_v1.PublishClient()
topic_path = publisher.topic_path(project_id, topic_id)
print(f"Topic Path {topic_path}")

for n in range(1, 25):
    data_string = {}