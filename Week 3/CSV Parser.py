import csv
import logging
from typing import Dict, List, Any

# Configure logger (must follow existing logging standards in pipeline)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CSVParser:
    """
    CSV Parser Module for Project Atlas
    Responsible for parsing client-provided CSV files and
    preparing records for legacy validation.
    """

    def __init__(self, required_fields: List[str]):
        """
        :param required_fields: Core fields expected in the CSV schema
        """
        self.required_fields = required_fields

    def parse(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parses the CSV file and returns valid records.

        :param file_path: Path to CSV file
        :return: List of parsed records
        """
        parsed_records = []

        try:
            with open(file_path, mode="r", newline="", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)

                for row_number, row in enumerate(reader, start=1):
                    try:
                        record = self._process_row(row, row_number)
                        if record:
                            parsed_records.append(record)
                    except Exception as row_error:
                        logger.error(
                            f"Row {row_number} skipped due to error: {row_error}"
                        )

        except FileNotFoundError:
            logger.error(f"CSV file not found: {file_path}")
        except Exception as file_error:
            logger.error(f"Failed to parse CSV file: {file_error}")

        logger.info(f"Successfully parsed {len(parsed_records)} records")
        return parsed_records

    def _process_row(self, row: Dict[str, str], row_number: int) -> Dict[str, Any]:
        """
        Validates and processes a single CSV row.

        :param row: Raw CSV row
        :param row_number: Row index in CSV
        :return: Processed record or None
        """

        # Check for required fields
        for field in self.required_fields:
            if field not in row or row[field] == "":
                raise ValueError(
                    f"Missing required field '{field}' at row {row_number}"
                )

        # Basic type conversions (can be extended later)
        processed_record = {}

        for key, value in row.items():
            processed_record[key] = value.strip() if isinstance(value, str) else value

        return processed_record
