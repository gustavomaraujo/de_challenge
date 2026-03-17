"""
Validate CSV operator: checks file exists and schema (region, origin_coord, destination_coord, datetime, datasource).
Fails DAG if invalid.
"""
import csv
import os
import re

from airflow.models import BaseOperator


class ValidateCsvOperator(BaseOperator):
    template_fields = ["file_path"]

    def __init__(self, file_path: str, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path

    def execute(self, context):
        base = "/opt/airflow"
        path = self.file_path if os.path.isabs(self.file_path) else os.path.join(base, self.file_path)

        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file not found: {path}")

        required_columns = {"region", "origin_coord", "destination_coord", "datetime", "datasource"}
        point_re = re.compile(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)")

        with open(path, "r") as f:
            reader = csv.DictReader(f)
            cols = set(reader.fieldnames or [])
            missing = required_columns - cols
            if missing:
                raise ValueError(f"CSV missing required columns: {missing}")

            for i, row in enumerate(reader):
                if i >= 3:
                    break
                for key in ["origin_coord", "destination_coord"]:
                    val = row.get(key, "")
                    if not point_re.match(val.strip()):
                        raise ValueError(f"Invalid POINT format in row {i+2}: {val}")

        self.log.info(f"CSV validated: {path}")
        return path
