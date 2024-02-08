# Build shared libraries for linux and macosx, one will fail
go build -buildmode c-shared -o libweaviate.so .
go build -buildmode c-shared -o libweaviate.dylib .
# Try to build weaviate as a TCL library.  Requires the TCL development libraries
gcc -dynamiclib -DUSE_TCL_STUBS bindings/tcl.c -L/Library/Developer/CommandLineTools/SDKs/MacOSX12.1.sdk/System/Library/Frameworks/Tcl.framework -L/Library/Developer/CommandLineTools/SDKs/MacOSX13.3.sdk/System/Library/Frameworks/Tcl.framework/Versions/8.5/ -L/Library/Developer/CommandLineTools/SDKs/MacOSX12.1.sdk/System/Library/Frameworks/Tcl.framework/Versions/8.5/ -L/Library/Developer/CommandLineTools/SDKs/MacOSX12.3.sdk/System/Library/Frameworks/Tcl.framework/Versions/8.5/  -L. -I. -lweaviate -ltclstub -o libweaviateTCL.dylib

#Now load tclsh and run load [file join [pwd] libweaviateTCL[info sharedlibextension]]
echo start tclsh and run
echo load [file join [pwd] libweaviateTCL[info sharedlibextension]]
