import torch
import json
import nltk
from nltk.corpus import words
import random
from uuid import uuid4

nltk.download('words')
word_list = words.words()

limit = 100000

vecs = []
vectors = torch.rand(limit, 256)
for v in vectors:
   vecs+=[v.tolist()]

objs = []

for i in range(0,limit):
    if i % 100 == 0:
        print("generated {} items".format(i))
    title = ' '.join([random.choice(word_list) for i in range(0, 25)])
    text = ' '.join([random.choice(word_list) for i in range(0, 25)])
    token = ' '.join([random.choice(word_list) for i in range(0, 25)])

    objs+=[{
        'id': str(uuid4()),
        'properties': {
            'title': title,
            'text': text,
            'token': token,
            'itemId': int(i/1000),
            'itemIdHundred': int(i/100),
            'itemIdTen': int(i/10),

        },
        'vector': vecs[i]
    }]


with open("data.json", "w") as f:
    json.dump(objs, f)
