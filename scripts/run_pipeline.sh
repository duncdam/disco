#!/bin/zsh

if [[ ! -d ./resources/stage_0_outputs ]];
  then 
    mkdir ./resources/stage_0_outputs
fi

if [[ ! -d ./resources/stage_1_outputs ]];
  then 
    mkdir ./resources/stage_1_outputs
fi

if [[ ! -d ./resources/stage_2_outputs ]];
  then 
    mkdir ./resources/stage_2_outputs
fi

if [[ ! -d ./resources/stage_3_outputs ]];
  then 
    mkdir ./resources/stage_3_outputs
fi


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

echo "Staging altLabel relationship"
clj -X:altLabel
echo "Finish altLabel relationship"

clj -M:stage-2

echo "###################################"
echo "STAGE 2 FINISHED"                 
echo "###################################"

echo "###################################"
echo "STARTING STAGE 3"                 
echo "###################################"

clj -X:stage-3

echo "###################################"
echo "STAGE 3 FINISHED"                 
echo "###################################"