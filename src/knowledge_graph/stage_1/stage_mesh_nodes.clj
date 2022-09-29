(ns knowledge-graph.stage-1.stage-mesh-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-mesh-descriptor
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)]
      (->>
       (kg/csv->map data)
       (filter #(str/includes? (:tree_location %) "C"))
       (map #(assoc % :source_id (:id %)))
       (map #(assoc % :id (str/join ["MESH_" (:id %)])))
       (map #(assoc % :name (:label %)))
       (map #(assoc % :label "MESH"))
       (map #(assoc % :source "MESH"))
       (mapv #(select-keys % [:id :label :name :source_id :source]))
       distinct))))

(defn get-mesh-scr
  [scr-file-path des-file-path]
  (let [scr (->> (csv/read-csv (io/reader (io/resource scr-file-path)) :separator \tab)
                 (kg/csv->map))
        des (->> (csv/read-csv (io/reader (io/resource des-file-path)) :separator \tab)
                 (kg/csv->map)
                 (map #(select-keys % [:id :tree_location])))]
        (->>
          (kg/joiner scr des :heading_mapped :id kg/left-join)
          (filter #(some? (:tree_location %)))
          (filter #(str/includes? (:tree_location %) "C"))
          (map #(assoc % :source_id (:id %)))
          (map #(assoc % :name (:label %)))
          (map #(assoc % :id (str/join ["MESH_" (:id %)])))
          (map #(assoc % :label  "MESH"))
          (map #(assoc % :source "MESH"))
          (mapv #(select-keys % [:id :label :name :source_id :source]))
          distinct)))

(defn run []
  (let [des-file "stage_0_outputs/mesh_descriptor.csv"
        scr-file "stage_0_outputs/mesh_scr.csv"
        mesh-des (get-mesh-descriptor des-file)
        mesh-scr (get-mesh-scr scr-file des-file)
        mesh-nodes (concat mesh-des mesh-scr)]
    (->>
      (distinct  mesh-nodes)
      (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/mesh_nodes.csv"))))