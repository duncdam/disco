#!/usr/bin/zsh

cleanShell()
{
  #clean shell
  if [[ ! -z "$ZSH_VERSION" ]];
    then
      exec zsh
  elif [[ ! -z "$BASH_VERSION" ]];
    then 
      exec bash --login
  fi
}

while getopts "s:" opt
do
   case "$opt" in
      s ) SOURCE=$OPTARG ;;
   esac
done

if [[ -z $SOURCE ]]; then
  SOURCE="all"
fi

SNOMED_ICD10_URL=https://download.nlm.nih.gov/mlb/utsauth/ICD10CM/SNOMED_CT_to_ICD-10-CM_Resources_20220901.zip
SNOMED_CORE_URL=https://download.nlm.nih.gov/umls/kss/SNOMEDCT_CORE_SUBSET/SNOMEDCT_CORE_SUBSET_202205.zip
ICD9_URL=https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/ICD-9-CM-v32-master-descriptions.zip
SNOMED_ICD9_URL=https://download.nlm.nih.gov/umls/kss/mappings/ICD9CM_TO_SNOMEDCT/ICD9CM_DIAGNOSIS_MAP_202112.zip
SNOMED_ORPHANET_URL=https://download.nlm.nih.gov/umls/kss/IHTSDO20210731/SnomedCT_SNOMEDOrphanetMapPackage_PRODUCTION_20211031T120000Z.zip
SNOMED_MedDRA_URL=https://download.nlm.nih.gov/umls/kss/mappings/SnomedCT_SNOMEDMedDRAMapPackage_PRODUCTION_20220511T120000Z.zip
SNOMED_MAPSET_URL=https://download.nlm.nih.gov/mlb/utsauth/CMT/2022/UMLS_KP_ProblemList_Mapping_20220301_Final.txt
UMLS_URL=https://download.nlm.nih.gov/umls/kss/2022AA/umls-2022AA-metathesaurus.zip


if [[ $SOURCE == "umls" ]]; then
  DOWNLOAD_URLS=($UMLS_URL)
elif [[ $SOURCE == "snomedct" ]]; then
  DOWNLOAD_URLS=($SNOMED_ICD10_URL $SNOMED_CORE_URL $SNOMED_ORPHANET_URL $SNOMED_MedDRA_URL $SNOMED_ORPHANET_URL $SNOMED_MAPSET_URL)
elif [[ $SOURCE == "icd9" ]]; then
  DOWNLOAD_URLS=($SNOMED_ICD9_URL $ICD9_URL)
elif [[ $SOURCE == "all" ]]; then
  DOWNLOAD_URLS=($SNOMED_ICD10_URL $SNOMED_CORE_URL $SNOMED_ORPHANET_URL $SNOMED_MedDRA_URL $SNOMED_ORPHANET_URL $SNOMED_MAPSET_URL $SNOMED_ICD9_URL $ICD9_URL $UMLS_URL)
else 
  echo "Wrong source"
fi

API_KEY=$(echo $(cat .env | rev |  cut -d"=" -f 1 | rev) )

NLM_END_POINT="https://uts-ws.nlm.nih.gov/download"
OUTPUT_PATH=./resources/downloads

if [[ ! -d $OUTPUT_PATH ]]; then
  mkdir $OUTPUT_PATH
fi

for URL in $DOWNLOAD_URLS[@]; do 
  FILE=$(echo $URL | rev | cut -d"/" -f 1 | rev)

  if [[ $URL == *".nlm"* ]]; then
    curl "$NLMEND_POINT?url=$URL&apiKey=$API_KEY" -o $OUTPUT_PATH/$FILE
  else 
    curl $URL -o $OUTPUT_PATH/$FILE
  fi

  if [[ $FILE == *".zip"* ]]; then
    unzip -o -d $OUTPUT_PATH $OUTPUT_PATH/$FILE
  fi

  rm $OUTPUT_PATH/$FILE
done

cleanShell