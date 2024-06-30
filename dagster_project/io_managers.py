from dagster import IOManager, ConfigurableIOManagerFactory, InputContext, OutputContext, MetadataValue
from dagster import TableRecord, TableColumn, TableSchema

from pyiceberg.catalog.sql import SqlCatalog, NoSuchTableError

import pyiceberg as iceberg

import pandas as pd
import pyarrow as pa

# class TypedIOManager(IOManager):
#     def __init__(s):
        

#     def handle_output(self, context, obj):
#         pass
#     def load_input(self, context):
#         pass
    
class IcebergIOManagerFactory(ConfigurableIOManagerFactory):
    py_io_impl:str = "pyiceberg.io.fsspec.FsspecFileIO" #"pyiceberg.io.pyarrow.PyArrowFileIO" pyarrow only supports https scheme

    metadata_db_url: str
    s3_endpoint: str
    s3_access_key_id: str
    s3_secret_access_key: str
    warehouse: str
    namespace: str

    def _create_catalog(self):
        return SqlCatalog(
            "default",
            **{
                "uri": self.metadata_db_url,
                "s3.endpoint": self.s3_endpoint,
                "py-io-impl": self.py_io_impl,
                "s3.access-key-id": self.s3_access_key_id,
                "s3.secret-access-key": self.s3_secret_access_key,
                "warehouse": self.warehouse,
            },
        )

    def create_io_manager(self, context) -> IOManager:
        catalog = self._create_catalog()
        return IcebergIOManager(catalog, self.namespace)


class IcebergIOManager(IOManager):
    def __init__(self, catalog: SqlCatalog, namespace: str):
        self._catalog = catalog
        self._namespace = namespace

    def _table_name(self, name: str):
        return f"{self._namespace}.{name}"

    def _get_or_create_table(self, name: str, schema: iceberg.schema.Schema) -> iceberg.table.Table:
        table_name = self._table_name(name)

        try:
            return self._catalog.load_table(table_name)
        except NoSuchTableError:
            return self._catalog.create_table(table_name, schema=schema)
        
    def _table_column(name: str, dtype: str) -> TableColumn:
        return TableColumn(name=name, type="dtype")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        if isinstance(obj, pd.DataFrame):
            input = pa.Table.from_pandas(obj)
        else:
            raise Exception(f"Unrecognized type: {type(obj)}")
        
        table_name = context.asset_key.to_python_identifier()
        table = self._get_or_create_table(table_name, input.schema)

        table.overwrite(input)

        preview_records = obj.head().to_dict(orient='records')
        previews = [ { k:str(v) for k,v in r.items()} for r in preview_records]

        context.add_output_metadata({
            "dagster/row_count": MetadataValue.int(input.num_rows), 
            "table_name": MetadataValue.text(self._table_name(table_name)),
            "location": MetadataValue.url(table.metadata.location),
            "current_snapshot_id": MetadataValue.text(str(table.metadata.current_snapshot_id)),
            # "preview": MetadataValue.md(obj.head().to_markdown()),
            # "schema": TableSchema(
            #     columns=[
            #         TableColumn(name=c.name, type=str(c.field_type)) for c in table.metadata.schemas[0].columns
            #     ]
            # )
            "preview": MetadataValue.table(
                records = [
                    TableRecord(d) for d in previews
                ],
                schema = TableSchema(
                    columns=[
                        TableColumn(name=c.name, type=str(c.field_type)) for c in table.metadata.schemas[0].columns
                    ]
                )
            ),
        })

    def load_input(self, context: InputContext) -> pd.DataFrame:
        table_name = context.asset_key.to_python_identifier()
        table = self._catalog.load_table(self._table_name(table_name))

        return table.scan().to_pandas()


