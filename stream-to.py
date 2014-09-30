#!/usr/bin/env python

# Streams data to Kinesis

import boto.utils
import boto.kinesis
import sys
import boto

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

# Read data from stdin
line = sys.stdin.readline()

# Loop until there is no data left
while line:
  # Put the data into Kinesis
  kinesis.put_record(stream_name, line, stream_name)

  # Read the next line
  line = sys.stdin.readline()
