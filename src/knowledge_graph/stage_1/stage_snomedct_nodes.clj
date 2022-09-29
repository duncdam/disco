(ns knowledge-graph.stage-1.stage-snomedct-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)]
      (->>
       (kg/csv->map data)
       (map #(assoc % :id (str/join ["SNOMEDCT_" (:snomed_id %)])))
       (map #(assoc % :source_id (:snomed_id %)))
       (map #(assoc % :label "SNOMEDCT"))
       (map #(assoc % :source "SNOMEDCT"))
       (map #(assoc % :name (:snomed_name %)))
       (mapv #(select-keys % [:id :label :name :source_id :source]))
       distinct))))

(defn run []
  (let [snomed (get-results "stage_0_outputs/snomedct_icd10.csv")]
    (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/snomedct_nodes.csv" snomed)))