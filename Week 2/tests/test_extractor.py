"""
Unit Tests for Feature Extraction Module – Project Atlas (Week 2)
"""

import pytest
from pyspark.sql import SparkSession
from extractor import FeatureExtractor, FeatureExtractionError


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("feature-extractor-test")
        .getOrCreate()
    )


@pytest.fixture
def extractor():
    """FeatureExtractor instance."""
    return FeatureExtractor(text_column="text")


def test_missing_text_column_raises_error(spark, extractor):
    """
    Verify that missing text column raises a FeatureExtractionError.
    """
    df = spark.createDataFrame(
        [("sample log entry",)],
        ["message"]
    )

    with pytest.raises(FeatureExtractionError):
        extractor.extract_features(df)


def test_empty_text_handling(spark, extractor):
    """
    Verify handling of empty text input.
    """
    df = spark.createDataFrame(
        [("",)],
        ["text"]
    )

    result = extractor.extract_features(df).collect()[0]

    assert result.text_length == 0
    assert result.word_count == 0
    assert result.empty_text_flag == 1


def test_normal_text_feature_extraction(spark, extractor):
    """
    Verify correct feature extraction for normal text.
    """
    df = spark.createDataFrame(
        [("System error occurred in module",)],
        ["text"]
    )

    result = extractor.extract_features(df).collect()[0]

    assert result.text_length > 0
    assert result.word_count > 0
    assert result.empty_text_flag == 0
    assert result.keyword_density >= 0


def test_pii_sanitization_email_removal(spark, extractor):
    """
    Verify that emails are removed during sanitization.
    """
    df = spark.createDataFrame(
        [("Contact admin at test@example.com",)],
        ["text"]
    )

    sanitized_df = extractor.sanitize_text(df)
    sanitized_text = sanitized_df.collect()[0]["sanitized_text"]

    assert "@" not in sanitized_text


def test_numeric_identifier_removal(spark, extractor):
    """
    Verify numeric identifiers are removed.
    """
    df = spark.createDataFrame(
        [("User ID 12345 failed login",)],
        ["text"]
    )

    sanitized_df = extractor.sanitize_text(df)
    sanitized_text = sanitized_df.collect()[0]["sanitized_text"]

    assert "12345" not in sanitized_text


def test_multiple_rows_processing(spark, extractor):
    """
    Verify module processes multiple rows correctly.
    """
    df = spark.createDataFrame(
        [
            ("first log entry",),
            ("second log entry",),
            ("",)
        ],
        ["text"]
    )

    result_df = extractor.extract_features(df)
    results = result_df.collect()

    assert len(results) == 3
    assert results[2].empty_text_flag == 1
