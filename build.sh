#!/bin/zsh

while getopts "s:" opt; do
  case "$opt" in
  s) SOURCE=$OPTARG ;;
  esac
done

if [[ -z $SOURCE ]]; then
  SOURCE="all"
fi

echo "DOWNLOAD ... "

source ./scripts/download.sh -s $SOURCE

echo "FINISH DOWNLOADING!"

echo "RUN PIPELINE ..."

source ./scripts/run_pipeline.sh

echo "FINISH RUNNING PIPELINE!"

echo "CREATE NEO4J GRAPH DATABASE FOR DISEASE MAPPING"

source ./scripts/build_service.sh

exec zsh