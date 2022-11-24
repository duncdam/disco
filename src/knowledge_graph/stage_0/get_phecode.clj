(ns knowledge-graph.stage-0.get-phecode
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-phecode-info
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file)
         kg/csv->map
         (map #(select-keys % [:phecode :phenotype]))
         (map #(set/rename-keys % {:phecode :source_id :phenotype :label}))
         (map #(assoc % :id (str/replace (:source_id %) "." "")))
         (map #(assoc % :id (str/join "_" ["PHECODE" (:id %)])))
         (map #(assoc % :subClassOf (first (str/split (:source_id %) #"\."))))
         (map #(assoc % :synonym ""))
         (mapv #(select-keys % [:id :label :source_id :subClassOf :synonym])))))

(defn get-phecode-mapping
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (map #(assoc % :source_id (:phecode %)))
         (map #(assoc % :hasDbXref (str/replace (:ICD %) "." "")))
         (map #(assoc % :dbXref_source (cond
                                         (= (:flag %) "9") "ICD9CM"
                                         :else "ICD10CM")))
         (mapv #(select-keys % [:source_id :hasDbXref :dbXref_source])))))

(def phecode-file-path "downloads/phecode_definitions1.2.csv")
(def phecode-file-mapping "downloads/ICD-CM to phecode, unrolled.txt")
(def output-path "./resources/stage_0_outputs/phecode.csv")

(defn run
  [_]
  (let [phecode-info (get-phecode-info phecode-file-path)
        phecode-mapping (get-phecode-mapping phecode-file-mapping)]
    (->> (kg/joiner phecode-info phecode-mapping :source_id :source_id kg/left-join)
         distinct
         (kg/write-csv [:id :label :source_id :subClassOf :synonym :hasDbXref :dbXref_source] output-path))))
