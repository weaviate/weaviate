from ctypes import *
lib = CDLL('libweaviate.so')

from numpy.ctypeslib import ndpointer
# Define the types of the output and arguments of
# this function.
lib.startWeaviate.restype = None
lib.startWeaviate.argtypes = [c_void_p, c_void_p, c_void_p, c_void_p]  # cast everything to void*, lalala



lib.startWeaviate('./data', 'http://localhost:8080', 'node1', 'none')


lib.appClasses.restype = c_char_p
lib.appClasses.argtypes = []
classes_str = lib.appClasses()

classes = classes_str.decode('utf-8').split('\n')

lib.classProperties.restype = c_char_p
lib.classProperties.argtypes = [c_void_p]

#iterate over classes, print properties
for c in classes:
    print(c)
    properties_str = lib.classProperties(c)
    properties = properties_str.decode('utf-8').split('\n')
    for p in properties:
        print('\t', p)



#lib.dumpBucket.restype = None
#lib.dumpBucket.argtypes = [c_void_p, c_void_p] # absolutely nothing will go wrong
#lib.dumpBucket('data/classa_vwAWcbt8fAfh_lsm', 'title')