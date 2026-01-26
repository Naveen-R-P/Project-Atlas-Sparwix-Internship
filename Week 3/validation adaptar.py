import logging
from typing import List, Dict, Any

# Import legacy validation framework (DO NOT MODIFY legacy code)
from legacy_validation_framework import validate_record

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ValidationAdapter:
    """
    Adapter layer between CSV Parser and Legacy Validation Framework.
    Ensures compatibility without changing legacy validation logic.
    """

    def __init__(self):
        pass

    def validate(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validates parsed records using the legacy validation framework.

        :param records: Parsed CSV records
        :return: List of validated records
        """
        validated_records = []

        for index, record in enumerate(records, start=1):
            try:
                is_valid, errors = validate_record(record)

                if is_valid:
                    validated_records.append(record)
                else:
                    logger.warning(
                        f"Record {index} failed validation: {errors}"
                    )

            except Exception as validation_error:
                logger.error(
                    f"Validation error for record {index}: {validation_error}"
                )

        logger.info(
            f"Validation completed. {len(validated_records)} records passed validation."
        )
        return validated_records
