go build -buildmode c-shared -o libweaviate.so .
cp libweaviate.so libweaviate.dylib
gcc -L . -I . -lweaviate -ltcl -shared -o libweaviatetcl.dylib bindings/tcl.c
#Now load tclsh and run load [file join [pwd] libweaviate[info sharedlibextension]]
echo start tclsh and run
echo load [file join [pwd] libweaviate[info sharedlibextension]]
