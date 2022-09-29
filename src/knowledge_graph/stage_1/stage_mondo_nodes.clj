(ns knowledge-graph.stage-1.stage-mondo-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-mondo-from-mondo
  [file-path]
  (with-open [f (io/reader (io/resource file-path))]
    (let [data (csv/read-csv f :separator \tab)]
      (->>
       (kg/csv->map data)
       (map #(assoc % :id (last (str/split (:id %) #"/"))))
       (map #(assoc % :source_id (str/replace (:id %) #"_" ":")))
       (map #(assoc % :name (:label %)))
       (map #(assoc % :source "MONDO"))
       (map #(assoc % :label "MONDO"))
       (mapv #(select-keys % [:id :label :name :source_id :source]))
       distinct))))

(defn get-mondo-from-efo
  [file-path]
  (with-open [f (io/reader (io/resource file-path))]
    (let [data (csv/read-csv f :separator \tab)]
      (->>
       (kg/csv->map data)
       (filter #(str/includes? (:id %) "MONDO_"))
       (map #(assoc % :id (last (str/split (:id %) #"/"))))
       (map #(assoc % :source_id (str/replace (:id %) #"_" ":")))
       (map #(assoc % :name (:label %)))
       (map #(assoc % :source "MONDO"))
       (map #(assoc % :label "MONDO"))
       (mapv #(select-keys % [:id :label :name :source_id :source]))
       distinct))))

(defn run []
  (let [mondo-mondo (get-mondo-from-mondo "stage_0_outputs/mondo.csv")
        efo-mondo (get-mondo-from-efo "stage_0_outputs/efo.csv")
        mondo-nodes (concat mondo-mondo efo-mondo)]
    (->>
     (distinct mondo-nodes)
     (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/mondo_nodes.csv"))))