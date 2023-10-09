#include <tcl.h>
#include <stdio.h>
#include <libweaviate.h>


static int dumpBucket_tcl(ClientData clientData,
        Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
    printf("called with %d arguments\n", objc);
    return TCL_OK;
}

static int startWeaviate_tcl(ClientData clientData,
        Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
    printf("called with %d arguments\n", objc);
    return TCL_OK;
}


int Weaviate_Init(Tcl_Interp *interp) {
    if (Tcl_InitStubs(interp, "8.1", 0) == NULL) {
	return TCL_ERROR;
    }
    printf("creating foo command");
    Tcl_CreateObjCommand(interp, "startWeaviate", startWeaviate_tcl, NULL, NULL);
    Tcl_CreateObjCommand(interp, "dumpBucket", dumpBucket_tcl, NULL, NULL);
    return TCL_OK;
}
