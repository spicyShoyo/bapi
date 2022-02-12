#!/bin/bash
# builds frontend static resource to be served by the webserver
cd ../frontend/; npm run build -- --outDir ../service/cmd/webserver/static --emptyOutDir; cd -