(ns knowledge-graph.stage-1.stage-meddra-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [file-path]
  (with-open [f (io/reader (io/resource file-path))]
    (let [data (csv/read-csv f :separator \tab)]
      (->>
       (kg/csv->map data)
       (map #(assoc % :id (str/join  ["MEDDRA_" (:meddra_code %)])))
       (map #(assoc % :label "MEDDRA"))
       (map #(assoc % :source "MEDDRA"))
       (map #(set/rename-keys % {:meddra_code :source_id
                                 :meddra_name :name}))
       (mapv #(select-keys % [:id :label :name :source_id :source]))
       distinct))))

(defn run []
  (let [meddra (get-results "stage_0_outputs/ncit_meddra_mapping.csv")]
     (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/meddra_nodes.csv" meddra)))