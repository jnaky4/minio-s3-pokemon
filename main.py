from minio import Minio
from bulkboto3 import BulkBoto3, StorageTransferPath


TARGET_BUCKET = "pokemon"
NUM_TRANSFER_THREADS = 50
TRANSFER_VERBOSITY = True

# instantiate a BulkBoto3 object
bulkboto_agent = BulkBoto3(
    resource_type="s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="poke",
    aws_secret_access_key="pokemon123",
    max_pool_connections=300,
    verbose=TRANSFER_VERBOSITY,
)


# create a new bucket
# bulkboto_agent.create_new_bucket(bucket_name=TARGET_BUCKET)
def upload_images():
    # upload a whole directory with its structure to an S3 bucket in multi thread mode
    bulkboto_agent.upload_dir_to_storage(
        bucket_name=TARGET_BUCKET,
        local_dir="Pokemon",
        storage_dir="pokemon",
        n_threads=NUM_TRANSFER_THREADS,
    )


def list_buckets():
    images = bulkboto_agent.list_objects(
        bucket_name=TARGET_BUCKET,
        storage_dir="pokemon"
    )
    print(images)


def get_image(file):
    client = Minio(
        endpoint="localhost:9000",
        access_key="poke",
        secret_key="pokemon123",
        secure=False,
    )
    bucket = "pokemon"

    client.fget_object(
        bucket_name=bucket,
        object_name=file,
        file_path="test\\" + file.split('\\')[-1],
    )


if __name__ == "__main__":
    # upload_images()
    # list_buckets()
    get_image('pokemon\\001Bulbasaur.png')

