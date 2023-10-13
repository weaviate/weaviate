#include <tcl.h>
#include <stdio.h>
#include <libweaviate.h>


static int dumpBucket_tcl(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
    printf("called with %d arguments\n", objc);
    return TCL_OK;
}

static int startWeaviate_tcl(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
    printf("called with %d arguments\n", objc);
    return TCL_OK;
}


int DLLEXPORT Weaviatetcl_Init(Tcl_Interp *interp) {
    printf("Initialising TCL library libweaviateTCL\n");
    if (Tcl_InitStubs(interp, "8.1", 0) == NULL) {
    	printf("Incorrect TCL version\n");
	return TCL_ERROR;
    }
    printf("creating startWeaviate command\n");
    Tcl_CreateObjCommand(interp, "startWeaviate", startWeaviate_tcl, NULL, NULL);
    printf("creating dumpBucket command\n");
    Tcl_CreateObjCommand(interp, "dumpBucket", dumpBucket_tcl, NULL, NULL);
    return TCL_OK;
}
