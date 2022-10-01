import pandas as pd
import warnings

warnings.simplefilter("ignore")

def read_ulms(concept_file_path, sematic_file_path):

  #read umls raw file in as csv with separator = '|'
  df_concept =pd.read_csv(concept_file_path, sep ='|', header=None, index_col=None)
  #add header
  df_concept.columns = ["CUI", "LAT", "TS", "LUI", "STT", "SUI", "ISPREF", "AUI", "SAUI", "SCUI", "SDUI", "SAB", "TTY", "CODE", "STR", "SRL", "SUPPRESS", "CVF", "empty"]
  #filter for only english term and with specific source
  df_concept_eng = df_concept[
    ((df_concept["LAT"] == "ENG") | (df_concept["TTY"] == "PN")) 
    & (df_concept["SAB"].isin(["MSH", "HPO", "ICD9CM", "ICD10CM", "MDR", "NCI", "SNOMEDCT_US", "MTH"]))
  ]

  #load semantic type file
  df_semantic = pd.read_csv(sematic_file_path, sep ="|", header = None, index_col = None)
  #add header
  df_semantic.columns = ["CUI", "TUI", "STN", "STY", "ATUI", "CVF", "empty"]
  #only keep CUI and STY column
  df_semantic = df_semantic[["CUI", "STY"]]


  #add semantic type to concept
  df_eng = (
    df_concept_eng
    .merge(
      right = df_semantic,
      how = "left",
      on = "CUI"
    )
  )

  #rewrite file as csv
  df_eng.drop(columns=['empty']).to_csv('./resources/umls/umls_eng_raw.csv', header=True, sep="\t", index=None)

if __name__ == "__main__":
  read_ulms('./resources/umls/MRCONSO.RRF', './resources/umls/MRSTY.RRF')