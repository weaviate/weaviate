dependencies:
- python v3.8.x
- weaviate_client v3.15.1

to run:
- $ docker-compose up -d
- $ python3 batch_data.py  

batch time: 16'35" (1), 12'39" (50), 12'45" (100)
batchless time: 56'14.5"

checking status/completion:
- $ python3 object_count.py

querying:
- $ python3 query.py # and follow instructions