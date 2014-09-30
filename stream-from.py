#!/usr/bin/env python

# Streams data from Kinesis

import boto.utils
import boto.kinesis
import boto
import time
from botocore.vendored.requests.packages.urllib3.exceptions import TimeoutError

# Get the instance metadata

instance_metadata = boto.utils.get_instance_metadata()

# Get our instance ID out of the metadata
instance_id = instance_metadata['instance-id']

# Use the instance ID as our stream name
stream_name = instance_id

# Use only one shard
shard_count = 1

try:
  # Connect to Kinesis
  kinesis = boto.connect_kinesis()

  # Create the stream for this instance ID
  kinesis.create_stream(stream_name, shard_count)
except boto.kinesis.exceptions.ResourceInUseException:
  # Stream has already been created, this can be safely ignored
  pass

tries = 0

# Try up to 10 times to open the stream
while tries < 10:
  tries += 1

  # Get the stream description
  response = kinesis.describe_stream(stream_name)

  # Is the stream active?
  if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
    # Yes, the stream is active and we are ready to start reading from it.  Get the shard ID.
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    break

  # The stream is not active, wait for 15 seconds and try again
  time.sleep(15)
else:
  # The stream took too long to become active
  raise TimeoutError('Stream is still not active, aborting...')

# Get the shard iterator and get only new data (TRIM_HORIZON)
response = kinesis.get_shard_iterator(stream_name, shard_id, 'TRIM_HORIZON')
shard_iterator = response['ShardIterator']

# Loop forever
while True:
  # Get the next set of records
  response = kinesis.get_records(shard_iterator)

  # Get the next shard iterator
  shard_iterator = response['NextShardIterator']

  # Loop through all of the records and print them
  for line in response['Records']:
    print line

  # Sleep for a second so we don't hit the Kinesis API too fast
  time.sleep(1)
