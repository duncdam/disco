#!bin/zsh
echo "###################################"
echo "DOWNLOADING..."                 
echo "###################################"

source ./scripts/source_download.sh -s all

echo "###################################"
echo "STARTING STAGE 0"                 
echo "###################################"

echo "Preprocessing UMLS data"
clj -X:umls
echo "Finished processing UMLS data"

clj -M:stage-0                  

echo "###################################"
echo "STAGE 0 FINISHED"                 
echo "###################################"

echo "\n"

echo "###################################"
echo "STARTING STAGE 1"                 
echo "###################################"

clj -M:stage-1

echo "###################################"
echo "STAGE 1 FINISHED"                 
echo "###################################"

echo "\n"

echo "###################################"
echo "STARTING STAGE 2"                 
echo "###################################"

clj -M:stage-2

echo "###################################"
echo "STAGE 2 FINISHED"                 
echo "###################################"