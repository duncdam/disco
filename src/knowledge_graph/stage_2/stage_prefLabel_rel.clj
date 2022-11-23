(ns knowledge-graph.stage-2.stage-prefLabel-rel
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-prefLabel
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (map #(assoc % :start (:id %)))
         (map #(assoc % :end (:name %)))
         (mapv #(select-keys % [:start :end]))
         distinct)))

(defn get-synonym-nodes
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (mapv #(select-keys % [:id :name]))
         distinct)))

(defn run [_]
  (let [synonym-nodes (get-synonym-nodes "stage_1_outputs/synonym_nodes.csv")
        prefLabel (get-prefLabel "stage_1_outputs/disease_nodes.csv")]
    (->> (kg/joiner prefLabel synonym-nodes :end :name kg/left-join)
         (map #(assoc % :type "prefLabel"))
         (map #(set/rename-keys % {:start :start_id :id :end_id}))
         (mapv #(select-keys % [:start_id :type :end_id]))
         (kg/write-csv [:start_id :type :end_id] "./resources/stage_2_outputs/prefLabel_rel.csv"))))