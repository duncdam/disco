(ns knowledge-graph.stage-1.stage-orphanet-nodes
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
       (filter #(some? (:id %)))
       (filter #(not= (:id %) ""))
       (map #(assoc % :id (last (str/split (:id %) #"/"))))
       (map #(assoc % :id (str/upper-case (:id %))))
       (map #(assoc % :label "ORPHANET"))
       (map #(assoc % :source "ORPHANET"))
       (mapv #(select-keys % [:id :label :name :source_id :source]))
       distinct))))

(defn run []
  (let [orphanet (get-results "stage_0_outputs/orphanet.csv")]
    (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/orphanet_nodes.csv" orphanet)))