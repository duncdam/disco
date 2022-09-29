(ns knowledge-graph.stage-0.get-icd10-icd9-mapping
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [url output-path]
  (->>
   (client/get url {:as :reader})
   :body
   csv/read-csv
   kg/csv->map
   (map #(select-keys % [:icd9cm :icd10cm :flags :approximate]))
   (filter #(not= (:id9cm %) ""))
   (kg/write-csv [:icd9cm :icd10cm :flags :approximate] output-path)
  ))

(defn run
  []
  (let [url "https://data.nber.org/gem/icd9toicd10cmgem.csv"
        output-path "./resources/stage_0_outputs/icd9_icd10_mapping.csv"]
    (get-results url output-path)))