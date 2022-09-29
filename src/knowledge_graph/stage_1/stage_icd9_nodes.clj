(ns knowledge-graph.stage-1.stage-icd9-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [file-path output-path]
  (with-open [f (io/reader (io/resource file-path))]
    (->>
     (csv/read-csv f :separator \tab)
     kg/csv->map
     (map #(select-keys % [:id :long_label]))
     (map #(assoc % :label "ICD9"))
     (map #(assoc % :source_id (:id %)))
     (map #(assoc % :source "ICD9CM"))
     (map #(assoc % :id (str/join ["ICD9_" (:id %)])))
     (map #(set/rename-keys % {:long_label :name}))
     (kg/write-csv [:id :label :name :source_id :source] output-path))))

(defn run []
  (let [file-path "stage_0_outputs/icd9.csv"
        output-path "./resources/stage_1_outputs/icd9_nodes.csv"]
    (get-results file-path output-path)))