import json
from google.cloud import pubsub_v1
from faker import Faker

project_id = "PROJECT_ID"
topic_id = "funTopic"

def publish_message(project_id, topic_id, message):
    publisher = pubsub_v1.PublisherClient()
    # create topic path
    topic = publisher.topic_path(project_id, topic_id)
    print(f'Topic {topic}')
    message_json = json.dumps(message).encode('utf-8')
    response = publisher.publish(topic=topic, data=message_json)
    print('Published Message ID {response.result()}')


if __name__ == "__main__":
    fake = Faker()
    for n in range(1, 25):
        data_string = {}
        data_string['name'] = fake.name()
        data_string['email'] = fake.email()
        data_string['latitude'] = fake.latitude()
        data_string['longitude'] = fake.longitude()
        # publish message
        publish_message(project_id, topic_id, data_string)

