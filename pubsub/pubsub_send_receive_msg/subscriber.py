import json
from google.cloud import pubsub_v1

def receive_json_message(project_id, subscriber_id, timeout=None):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscriber_id)

    def callback(message):
        message_data = json.loads(message.decode('utf-8'))
        print(f'Receiced message :{message_data}')
    
    future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        future.result(timeout=timeout)
    except KeyboardInterrupt:
        future.cancel()  # Trigger the shutdown.
        future.result()  # Block until the shutdown is complete.


if __name__ == "__main__":
    project_id = 'your-project-id'
    subscriber_id = 'your-subscriber_id'

    receive_json_message(project_id, subscriber_id)