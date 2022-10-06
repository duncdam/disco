(ns knowledge-graph.stage-0.get-icd9-mapping
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [icd10-url snomed-file-path output-path]
  (let [icd9-icd10-map (->> (client/get icd10-url {:as :reader})
                            :body
                            csv/read-csv
                            kg/csv->map
                            (filter #(not= (:id9cm %) ""))
                            (map #(assoc % :source "ICD10CM"))
                            (map #(set/rename-keys % {:icd9cm :id :icd10cm :hasDbXref }))
                            (mapv #(select-keys % [:id :hasDbXref :source])))
        icd9-snomed-map (with-open [file (io/reader (io/resource snomed-file-path))]
                            (->>  (slurp file)
                                  str/split-lines
                                  (map #(str/split % #"\t"))
                                  (cons ["ICD_CODE" "ICD_NAME" "IS_CURRENT_ICD" "IP_USAGE" "OP_USAGE" "AVG_USAGE" "IS_NEC" "SNOMED_CID" "SNOMED_FSN" "IS_1-1MAP" "CORE_USAGE" "IN_CORE" ])
                                  kg/csv->map
                                  (filter #(not (str/includes? (:ICD_CODE %) "ICD_CODE")))
                                  (map #(assoc % :hasDbXref (:SNOMED_CID %)))
                                  (map #(assoc % :source "SNOMED_CT"))
                                  (map #(assoc % :id (:ICD_CODE %)))
                                  (map #(assoc % :id (str/replace (:id %) "." "")))
                                  (mapv #(select-keys % [:id :hasDbXref :source]))))
        icd9-mapping (->> (concat icd9-icd10-map icd9-snomed-map)
                           distinct)]
        (kg/write-csv [:id :hasDbXref :source] output-path icd9-mapping)))

(defn run
  [_]
  (let [icd10-url "https://data.nber.org/gem/icd9toicd10cmgem.csv"
        snomed-file-path "download/ICD9CM_SNOMED_MAP_1TO1_202112.txt"
        output-path "./resources/stage_0_outputs/icd9_mapping.csv"]
    (get-results icd10-url snomed-file-path output-path)))