#!/bin/bash
echo "Caricamento file su docker-cloudera"
docker cp analyzer.jar cloudera:/DATA
echo "Eliminazione vecchia versione e upload nuovo analyzer su hdfs"
docker exec -it cloudera /DATA/uploadAnalyzerToHDFS.sh
echo "Upload completato"


#only test