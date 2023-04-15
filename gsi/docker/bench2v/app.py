import os
from logging import getLogger
from fastapi import FastAPI, Response, status
from vectorizer import Vectorizer, VectorInput
from meta import Meta
import dset

VERBOSE = int( os.getenv("VERBOSE") )
if VERBOSE: print("Got verbose env=", VERBOSE)

app = FastAPI()
logger = getLogger('uvicorn')
logger.propagate = True if VERBOSE else False

@app.on_event("startup")
def startup_event():
    dset.load()

@app.get("/.well-known/live", response_class=Response)
@app.get("/.well-known/ready", response_class=Response)
def live_and_ready(response: Response):
    response.status_code = status.HTTP_204_NO_CONTENT

@app.get("/meta")
def meta():
    return { 'model': "bench2v" }

@app.post("/vectors")
@app.post("/vectors/")
async def read_item(item: VectorInput, response: Response):

    global VERBOSE

    try:
        items = item.text.split()
        if VERBOSE: print("items=", items, "item=",item )

        if len(items)==1:
            searchtxt = items[0].strip()
        else: 
            searchtxt = items[-1].strip()

        if searchtxt.startswith("q-"):
            idx = int( searchtxt.split("-")[1] )
            vector, gt = dset.query(idx)
            if VERBOSE: print("query idx=",idx)
        else: 
            idx = int(searchtxt)
            vector = dset.get(idx)
            gt = None
            if VERBOSE: print("database idx=",idx)

        return {"text": item.text, "vector": vector.tolist(), "dim": len(vector)}

    except Exception as e:
        logger.exception(
            'Something went wrong while vectorizing data.'
        )
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"error": str(e)}
