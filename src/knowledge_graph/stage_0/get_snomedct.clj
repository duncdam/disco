(ns knowledge-graph.stage-0.get-snomedct
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-snomedct
  [file-path output-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \|)
         (kg/csv->map)
         (filter #(= (:SNOMED_CONCEPT_STATUS %) "Current"))
         (map #(set/rename-keys % {:SNOMED_CID :id :SNOMED_FSN :label}))
         (map #(assoc % :hasDbXref (:UMLS_CUI %)))
         (map #(assoc % :dbXref_source "UMLS"))
         (map #(select-keys % [:id :label :hasDbXref :dbXref_source]))
         distinct
         (map #(assoc % :id (str/join "_" ["SNOMEDCT" (:id %)])))
         (kg/write-csv [:id :label :hasDbXref :dbXref_source] output-path))))

(defn run [_]
  (let [file-path "downloads/SNOMEDCT_CORE_SUBSET_202205.txt"
        output-path "./resources/stage_0_outputs/snomedct.csv"]
      (get-snomedct file-path output-path)))