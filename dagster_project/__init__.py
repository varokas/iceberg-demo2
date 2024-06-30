from dagster import Definitions, load_assets_from_modules

from . import assets
from . import io_managers

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "iceberg_io_manager": io_managers.IcebergIOManagerFactory(
            metadata_db_url = "postgresql+psycopg2://postgres:mypostgrespassword@localhost/postgres",
            s3_endpoint = "http://localhost:9000",
            s3_access_key_id = "minio",
            s3_secret_access_key = "myminiosecret",
            warehouse = "s3://warehouse",
            namespace = "default",
        )
    }
)
