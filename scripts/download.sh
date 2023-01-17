#!/bin/zsh
cleanShell() {
  #clean shell
  if [[ ! -z "$ZSH_VERSION" ]]; then
    exec zsh
  elif [[ ! -z "$BASH_VERSION" ]]; then
    exec bash --login
  fi
}

while getopts "s:" opt; do
  case "$opt" in
  s) SOURCE=$OPTARG ;;
  esac
done

if [[ -z $SOURCE ]]; then
  SOURCE="all"
fi

#load .env variables
source .env

if [[ $SOURCE == "umls" ]]; then
  DOWNLOAD_URLS=($UMLS_URL)
elif [[ $SOURCE == "snomedct" ]]; then
  DOWNLOAD_URLS=(
    $SNOMED_ICD10_URL $SNOMED_CORE_URL $SNOMED_INT_URL
    $SNOMED_MedDRA_URL $SNOMED_ORPHANET_URL $SNOMED_MAPSET_URL
  )
elif [[ $SOURCE == "icd9" ]]; then
  DOWNLOAD_URLS=($SNOMED_ICD9_URL $ICD9_URL $ICD9_ICD10_MAPPING_URL)
elif [[ $SOURCE == "icd11" ]]; then
  DOWNLOAD_URLS=($ICD11_URL $ICD11_MAPPING_URL)
elif [[ $SOURCE == "icd10" ]]; then
  DOWNLOAD_URLS=($ICD10_URL)
elif [[ $SOURCE == 'icdo' ]]; then
  DOWNLOAD_URLS=($ICDO_URL)
elif [[ $SOURCE == "ncit" ]]; then
  DOWNLOAD_URLS=($NCIT_URL)
elif [[ $SOURCE == "mondo" ]]; then
  DOWNLOAD_URLS=($MONDO_URL)
elif [[ $SOURCE == "mesh" ]]; then
  DOWNLOAD_URLS=($MESH_DES_URL $MESH_SCR_URL)
elif [[ $SOURCE == "orphanet" ]]; then
  DOWNLOAD_URLS=($ORPHANET_URL $ORPHANET_SUBCLASS_URL)
elif [[ $SOURCE == "phecode" ]]; then
  DOWNLOAD_URLS=($PHECODE_URL $PHECODE_MAPPING_URL)
elif [[ $SOURCE == "all" ]]; then
  DOWNLOAD_URLS=(
    $SNOMED_ICD10_URL $SNOMED_CORE_URL $SNOMED_INT_URL $SNOMED_ORPHANET_URL $SNOMED_MedDRA_URL $SNOMED_ORPHANET_URL $SNOMED_MAPSET_URL $SNOMED_ICD9_URL 
    $ICD9_URL $ICD9_ICD10_MAPPING_URL $ICD11_URL $ICD11_MAPPING_URL $ICD10_URL $NCIT_URL $ICDO_URL $MONDO_URL $ORPHANET_URL $ORPHANET_SUBCLASS_URL 
    $MESH_DES_URL $MESH_SCR_URL $PHECODE_URL $PHECODE_MAPPING_URL $UMLS_URL 
  )
else
  echo "Wrong source"
fi

NLM_END_POINT="https://uts-ws.nlm.nih.gov/download"
OUTPUT_PATH=./resources/downloads

if [[ ! -d $OUTPUT_PATH ]]; then
  mkdir $OUTPUT_PATH
fi

for URL in $DOWNLOAD_URLS[@]; do
  FILE=$(echo $URL | rev | cut -d"/" -f 1 | rev)
  echo "Downloading $FILE"

  if [[ $URL == *"download.nlm.nih.gov"* ]]; then
    curl "$NLM_END_POINT?url=$URL&apiKey=$API_KEY" -o $OUTPUT_PATH/$FILE
  else
    curl $URL -o $OUTPUT_PATH/$FILE
  fi

  if [[ $FILE == *".zip"* ]]; then
    unzip -o -d $OUTPUT_PATH $OUTPUT_PATH/$FILE
    rm $OUTPUT_PATH/$FILE
  fi

done