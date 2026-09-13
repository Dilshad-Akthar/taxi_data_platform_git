from pyspark.sql.types import StructType

from yellow_trip_schema import YELLOW_TRIP_SCHEMA


class SchemaRegistry:
    """Resolves schema names to Spark StructType definitions."""

    _schemas = {
        "yellow_trip_schema": YELLOW_TRIP_SCHEMA,
    }

    def get_schema(self, schema_name: str) -> StructType:
        if schema_name not in self._schemas:
            raise ValueError(
                f"Unknown schema: {schema_name}"
            )

        return self._schemas[schema_name]