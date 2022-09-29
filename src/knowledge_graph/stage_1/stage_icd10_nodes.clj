(ns knowledge-graph.stage-1.stage-icd10-nodes
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
     (map #(set/rename-keys % {:label :name :id :source_id}))
     (map #(assoc % :label "ICD10"))
     (map #(assoc % :source "ICD10CM"))
     (map #(assoc % :id (str/join ["ICD10_" (:source_id %)])))
     distinct
     (kg/write-csv [:id :label :name :source_id :source] output-path))))

(defn run []
  (let [file-path "stage_0_outputs/icd10.csv"
        output-path "./resources/stage_1_outputs/icd10_nodes.csv"]
    (get-results file-path output-path)))