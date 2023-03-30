dependencies:
- python v3.8.x
- weaviate_client v3.15.1

to run:
- follow instructions under gsi/test directory for starting gsi/t2v docker container
- $ python3 gemini_nlp.py  

checking status/completion:
- $ python3 object_count.py

querying:
- for individual queries using Gemini similarity search
- $ python3 query.py # and follow instructions