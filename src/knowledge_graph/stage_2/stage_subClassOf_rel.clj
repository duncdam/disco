(ns knowledge-graph.stage-2.stage-subClassOf-rel
  (:require
    [clojure.java.io :as io]
    [clojure.set :as set]
    [clojure.data.csv :as csv]
    [knowledge-graph.module.module :as kg]))

(defn get-subClassOf-rel
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (map #(assoc % :start_id (:id %)))
         (map #(assoc % :end (:subClassOf %)))
         (mapv #(select-keys % [:start_id :end])))))

(defn disease-nodes
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (map #(set/rename-keys % {:id :end_id}))
         (mapv #(select-keys % [:end_id])))))

(defn run 
  [_]
  (let [doid-subClassOf (get-subClassOf-rel "stage_0_outputs/doid.csv")
        efo-subClassOf (get-subClassOf-rel "stage_0_outputs/efo.csv")
        hpo-subClassOf (get-subClassOf-rel "stage_0_outputs/hpo.csv")
        mesh-des-subClassOf (get-subClassOf-rel "stage_0_outputs/mesh_descriptor.csv")
        mesh-scr-subClassOf (get-subClassOf-rel "stage_0_outputs/mesh_scr.csv")
        mondo-subClassOf (get-subClassOf-rel "stage_0_outputs/mondo.csv")
        orphanet-subClassOf (get-subClassOf-rel "stage_0_outputs/orphanet.csv")
        subClassOf (concat doid-subClassOf efo-subClassOf hpo-subClassOf
                           mesh-des-subClassOf mesh-scr-subClassOf
                           mondo-subClassOf orphanet-subClassOf)
        disease-nodes (disease-nodes "stage_1_outputs/disease_nodes.csv")]
    (->> (kg/joiner subClassOf disease-nodes :end :end_id kg/inner-join)
         (map #(assoc % :type "subClassOf"))
         (mapv #(select-keys % [:start_id :type :end_id]))
         distinct
         (kg/write-csv [:start_id :type :end_id] "./resources/stage_2_outputs/subClassOf_rel.csv"))))