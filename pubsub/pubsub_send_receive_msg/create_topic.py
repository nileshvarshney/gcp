from google.cloud import pubsub_v1

def create_topic(project_id, topic_id):
    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(project_id, topic_id)
    response = publisher.create_topic(request={"name":topic })
    print(f"Created topic: {response.name}")


if __name__ == "__main__":
    project_id = "your-project-id"
    topic_id = "your-pubsub-topic-id"

    create_topic(project_id, topic_id)

