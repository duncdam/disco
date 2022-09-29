(ns knowledge-graph.stage-1.stage-medgen-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn create-medgen-node
  [file-path output-path]
  (with-open [f (io/reader (io/resource file-path))]
    (->>
     (csv/read-csv f :separator \tab)
     kg/csv->map
     (map #(select-keys % [:medgen_id :medgen_name]))
     (map #(assoc % :source_id (:medgen_id %)))
     (map #(set/rename-keys % {:medgen_id :id
                               :medgen_name :name}))
     (map #(assoc % :id (str/join ["MEDGEN_" (:id %)])))
     (map #(assoc % :label "MEDGEN"))
     (map #(assoc % :source "MEDGEN"))
     distinct
     (kg/write-csv [:id :label :name :source_id :source] output-path))))

(defn run
  []
  (let [file-path "stage_0_outputs/medgen_id_mapping.csv"
        output-path "./resources/stage_1_outputs/medgen_nodes.csv"]
    (create-medgen-node file-path output-path)))