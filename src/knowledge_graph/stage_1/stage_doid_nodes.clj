(ns knowledge-graph.stage-1.stage-doid-nodes
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
        (map #(assoc % :id (last (str/split (:id %) #"/"))))
        (map #(assoc % :source_id (str/replace (:id %) #"_" ":")))
        (map #(assoc % :name (:label %)))
        (map #(assoc % :label "DOID"))
        (map #(assoc % :source "DOID"))
        (mapv #(select-keys % [:id :label :name :source_id :source]))
        distinct))))


(defn run []
  (let [doid (get-results "stage_0_outputs/doid.csv")]
    (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/doid_nodes.csv" doid)))