(ns knowledge-graph.stage-3.stage-data
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]
   [taoensso.timbre :as log]))

(defn read-file
  [file-path]
  (with-open [f (io/reader (io/resource file-path))]
    (->> (csv/read-csv f :separator \tab)
         kg/csv->map
         vec)))

(defn -main
  []
  (let [diseases (read-file "stage_1_outputs/disease_nodes.csv")
        synonyms (read-file "stage_1_outputs/synonym_nodes.csv")
        altLabel (read-file "stage_2_outputs/altLabel_rel.csv")
        hasDbXref (read-file "stage_2_outputs/hasDbXref_rel.csv")
        prefLabel (read-file "stage_2_outputs/prefLabel_rel.csv")
        subClassOf (read-file "stage_2_outputs/subClassOf_rel.csv")
        relatedTo (read-file "stage_2_outputs/relatedTo_rel.csv")
        nodes (distinct (concat diseases synonyms))
        relationships (distinct (concat altLabel hasDbXref prefLabel subClassOf relatedTo))]
    (log/info "Staging all nodes for neo4j")
    (->> (map #(set/rename-keys % {:id :ID :label :LABEL}) nodes)
         (kg/write-csv [:LABEL :ID :name :source_id :source] "./resources/stage_3_outputs/nodes.csv"))
    (log/info "Staging all relationships for neo4j")
    (->> (map #(set/rename-keys % {:start_id :START_ID :type :TYPE :end_id :END_ID}) relationships)
         (kg/write-csv [:START_ID :TYPE :END_ID] "./resources/stage_3_outputs/relationships.csv"))))