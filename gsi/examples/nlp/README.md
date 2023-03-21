dependencies:
- python v3.8.x
- weaviate_client v3.15.1

to run:
- $ docker-compose up -d
- $ python3 batch_data.py #timeout error
- $ python3 batchless.py 

batch time: 16'35" (1), 12'39" (50)
batchless time: 56'14.5"

checking for completion:
- $ python3 object_count # should print 22k/22k

querying:
- $ python3 query.py # and follow instructions