from google.cloud import bigtable
from google.cloud.bigtable import column_family


project_id = 'playground-s-11-93efa806'
instance_id ='my-bigtable-instance'
cluster_id='my-bigtable-cluster'
table_name='my-bigtable'

def create_instance(instance_id, cluster_id, project_id):
    client = bigtable.Client(project_id = project_id, admin=True)
    instance = client.instance(instance_id)

    # create a cluster with the same ID as the instance
    cluster = instance.cluster(cluster_id, location='us-central1-f', service_nodes=1)

    if not instance.exists():
        print(f'Creating instance {instance_id}....')
        instance.create(clusters = [cluster])
        print(f'Instance {instance_id}. created...')
    else:
        print(f'Instance {instance_id} already exists.')

if __name__ == "__main__":
    create_instance(instance_id, cluster_id, project_id)


