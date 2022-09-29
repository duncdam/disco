(ns knowledge-graph.stage-1.stage-ncit-nodes
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
       (map #(assoc % :id (str/join  ["NCI_" (:ncit_id %)])))
       (map #(assoc % :label "NCIT"))
       (map #(assoc % :source "NCIT"))
       (map #(set/rename-keys % {:ncit_id :source_id
                                 :ncit_name :name}))
       (mapv #(select-keys % [:id :label :name :source_id :source]))
       distinct))))

(defn run []
  (let [ncit-from-ncit-meddra (get-results "stage_0_outputs/ncit_meddra_mapping.csv")
        ncit-from-ncit-neoplasm (get-results "stage_0_outputs/ncit_neoplasm_mapping.csv")
        ncit (concat ncit-from-ncit-meddra ncit-from-ncit-neoplasm)]
    (->>
     (distinct ncit)
     (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/ncit_nodes.csv"))))