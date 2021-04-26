import hnswlib
import numpy as np
import time
import json

data=None
queries=None
truths=None

with open("recall_vectors.json", 'r') as f:
    data = json.load(f)

with open("recall_queries.json", 'r') as f:
    queries = json.load(f)

with open("recall_truths.json", 'r') as f:
    truths = json.load(f)

num_elements = len(data)
dim = len(data[0])
data_labels = np.arange(num_elements)

# Declaring index
p = hnswlib.Index(space = 'cosine', dim = dim) # possible options are l2, cosine or ip

# Initializing index - the maximum number of elements should be known beforehand
p.init_index(max_elements = num_elements, ef_construction = 2000, M = 100)

before = time.time()
# Element insertion (can be called several times):
p.add_items(data, data_labels)
print("import took {}".format(time.time() - before))

# Controlling the recall by setting ef:
p.set_ef(100) # ef should always be > k

# Query dataset, k - number of closest elements (returns 2 numpy arrays)
results, distances = p.knn_query(queries, k = 1)

relevant=0
retrieved=0

for i, res in enumerate(results):
    retrieved+=1

    # take elem 0 because k==1
    if res[0] == truths[i][0]:
        relevant+=1

print("Recall: {}".format(relevant/retrieved))
