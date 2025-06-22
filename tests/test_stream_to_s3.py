import os
import tempfile
import polars as pl
import pytest

from stream_simulator.stream_to_s3 import load_checkpoint, save_checkpoint

def test_checkpoint_save_and_load():
    """Create a temporary checkpoint file to test correct saving and loading"""
    # checkpoint_file = tmp_path / "checkpoint.txt"
    save_checkpoint(42)
    result = load_checkpoint()  
    assert result == 42

def test_batch_splitting():
    """Test that the dataframe is split into batches correctly, even at the end of the dataframe"""
    # create a fake dataframe with 25 rows
    df = pl.DataFrame({"a": range(25)})
    batch_size = 10

    # to simulate batch splitting
    batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]

    # expect 3 batches: 2 full batches of 10 and 1 batch of 5
    assert len(batches) == 3
    assert len(batches[0]) == 10
    assert len(batches[1]) == 10
    assert len(batches[2]) == 5

# S3 integration test
from moto import mock_aws

@mock_aws
def test_s3_upload():
    """Test uploading a file to S3 using moto for mocking AWS services"""
    import boto3
    # Set up fake S3, upload and verify
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="test.txt", Body="hello world")
    resp = s3.list_objects(Bucket="test-bucket")
    keys = [obj["Key"] for obj in resp.get("Contents", [])]
    assert "test.txt" in keys
