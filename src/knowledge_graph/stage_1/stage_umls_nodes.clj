(ns knowledge-graph.stage-1.stage-umls-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)]
      (->>
       (kg/csv->map data)
       (filter #(= (:label_type %) "PN"))
       (map #(assoc % :id (str/join ["UMLS_" (:cuid %)])))
       (map #(assoc % :source_id (:cuid %)))
       (map #(assoc % :name (:label %)))
       (map #(assoc % :label "UMLS"))
       (map #(assoc % :source "UMLS"))
       (mapv #(select-keys % [:id :label :name :source_id :source]))
       distinct))))

(defn run []
  (let [umls (get-results "stage_0_outputs/umls.csv")]
    (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/umls_nodes.csv" umls)))