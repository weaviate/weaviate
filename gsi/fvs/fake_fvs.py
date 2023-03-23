#!/usr/bin/env python3
"""
License: MIT License
Copyright (c) 2023 Miel Donkers
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import json
import uuid
import numpy
import random

#
# configuration
#
VERBOSE = False

#
# globals
#
DATASET_ID = None
DS_FILE_PATH = None

class S(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        if VERBOSE: logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self._set_response()

        if VERBOSE: print("path",str(self.path),"end")
        if DATASET_ID and str(self.path).find("/v1.0/dataset/train/status")>=0:
            parts = str(self.path).split("/")
            if parts[-1]==DATASET_ID:
                jsonret = json.dumps( {"datasetStatus": "completed"} ).encode('utf-8')
                if VERBOSE: print("sending training status=", type(jsonret),jsonret)
                self.wfile.write( jsonret  )
            else:
                jsonret = json.dumps( {"status": "invalid dataset id"} ).encode('utf-8')
                if VERBOSE: print("sending training status=", type(jsonret),jsonret)
                self.wfile.write( jsonret  )
        else:
            self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))

    def do_POST(self):
        global DATASET_ID, DS_FILE_PATH

        content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
        post_data = self.rfile.read(content_length) # <--- Gets the data itself
        logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
                str(self.path), str(self.headers), post_data.decode('utf-8'))
        self._set_response()
        
        if VERBOSE: print("path",str(self.path),"end")
        if str(self.path) == "/v1.0/dataset/import":
            if not DATASET_ID: 
                DATASET_ID = str(uuid.uuid1())
            else: 
                raise Exception("Already got a dataset_id")
            body_dct = json.loads( post_data.decode('utf-8') )
            DS_FILE_PATH = body_dct["dsFilePath"]
            if VERBOSE: print("DS_FILE_PATH=",DS_FILE_PATH)
            a = numpy.load(DS_FILE_PATH)
            if VERBOSE: print(type(a))
            if VERBOSE: print(a.shape)
            jsonret = json.dumps( {"datasetId":DATASET_ID } ).encode('utf-8')
            if VERBOSE: print("sending datasetid=", type(jsonret),jsonret)
            self.wfile.write( jsonret  )
        elif DATASET_ID and str(self.path) == "/v1.0/dataset/load":
            jsonret = json.dumps( {"status":"ok" } ).encode('utf-8')
            if VERBOSE: print("sending loaded=", type(jsonret),jsonret)
            self.wfile.write( jsonret  )
        elif DATASET_ID and str(self.path) == "/v1.0/dataset/search":
            body_dct = json.loads( post_data.decode('utf-8') )
            Q_FILE_PATH = body_dct["queriesFilePath"]
            if VERBOSE: print("Q_FILE_PATH=",Q_FILE_PATH)
            q = numpy.load(Q_FILE_PATH)
            a = numpy.load(DS_FILE_PATH)
            idx = int( random.random() * a.shape[0] )
            vec = a[idx,:].tolist()
            srch = { 'indices': [[idx]], 'distance':[[1.0]], 'search':5.0 } 
            jsonret = json.dumps( srch ).encode('utf-8')
            if VERBOSE: print("sending search=", type(jsonret),jsonret)
            self.wfile.write( jsonret  )
        else:
            self.wfile.write("POST request for {}".format(self.path).encode('utf-8'))


def run(server_class=HTTPServer, handler_class=S, port=8080):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting httpd...\n')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...\n')

if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
