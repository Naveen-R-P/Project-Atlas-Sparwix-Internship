"""
Feature Extraction Module – Project Atlas (Week 2 Prototype)

Purpose:
    Extract lightweight, structured features from unstructured text data
    (logs, notes) for downstream analytical components.

Constraints:
    - Python + Apache Spark only
    - No external APIs
    - No PII exposure
    - Optimized for shared cluster usage
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, lower, regexp_replace, split, size
from pyspark.sql.types import IntegerType, FloatType


class FeatureExtractionError(Exception):
    """Custom exception for feature extraction failures."""
    pass


class FeatureExtractor:
    """
    Lightweight feature extractor for unstructured text data.
    """

    def __init__(self, text_column: str = "text"):
        """
        Args:
            text_column (str): Name of the column containing raw text.
        """
        self.text_column = text_column

    def validate_input(self, df: DataFrame) -> None:
        """
        Validate presence of required columns.

        Raises:
            FeatureExtractionError: If input schema is invalid.
        """
        if self.text_column not in df.columns:
            raise FeatureExtractionError(
                f"Missing required column: {self.text_column}"
            )

    def sanitize_text(self, df: DataFrame) -> DataFrame:
        """
        Sanitize text to reduce risk of PII leakage.
        Removes numbers, emails, and special characters.

        Returns:
            DataFrame: Sanitized DataFrame
        """
        sanitized = df.withColumn(
            "sanitized_text",
            regexp_replace(
                lower(col(self.text_column)),
                r"(\b\d+\b|[\w\.-]+@[\w\.-]+)",
                ""
            )
        )
        return sanitized

    def extract_features(self, df: DataFrame) -> DataFrame:
        """
        Extract structured features from unstructured text.

        Features:
            - text_length
            - word_count
            - keyword_density (basic signal)
            - empty_text_flag

        Returns:
            DataFrame: DataFrame with extracted features
        """
        self.validate_input(df)

        df_clean = self.sanitize_text(df)

        features_df = (
            df_clean
            .withColumn("text_length", length(col("sanitized_text")))
            .withColumn(
                "word_count",
                size(split(col("sanitized_text"), r"\s+"))
            )
            .withColumn(
                "keyword_density",
                (col("word_count") / col("text_length")).cast(FloatType())
            )
            .withColumn(
                "empty_text_flag",
                (col("text_length") == 0).cast(IntegerType())
            )
        )

        return features_df.select(
            col(self.text_column),
            col("text_length"),
            col("word_count"),
            col("keyword_density"),
            col("empty_text_flag")
        )
