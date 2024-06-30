from dagster import AssetKey, MaterializeResult, TableColumn, TableSchema, asset, DailyPartitionsDefinition

import pandas as pd

@asset(
    owners=["varokas@test.com", "team:data-eng"],
    io_manager_key="iceberg_io_manager"
)
def taxi() -> pd.DataFrame:
    return pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet")

@asset(
    owners=["varokas@test.com", "team:data-eng"],
    io_manager_key="iceberg_io_manager"
)
def taxi_passengers_by_vendor_id(context, taxi) -> pd.DataFrame:
    return taxi.groupby("VendorID")['passenger_count'].sum().reset_index()

# @asset(owners=["data@eng.com", "team:data-eng"],     metadata={
#         "dagster/column_schema": TableSchema(
#             columns=[
#                 TableColumn(
#                     "name",
#                     "string",
#                     description="The name of the person",
#                 ),
#                 TableColumn(
#                     "age",
#                     "int",
#                     description="The age of the person",
#                 ),
#             ]
#         )
#     }, partitions_def=DailyPartitionsDefinition(start_date="2024-06-21"))
# def hello() -> str:
#     return "hello"

# @asset(metadata={"dataset_name": "iris.small_petals", "schema": "a\nb"}, partitions_def=DailyPartitionsDefinition(start_date="2024-06-21"))
# def world(context, hello):
#     context.log.info(hello)
#     context.log.info("world")