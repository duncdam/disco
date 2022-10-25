(ns knowledge-graph.module.module
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :refer [difference intersection union]]))

(defn create-header
  [column is-stage]
  (if (true? is-stage)
    (if (contains? #{:ID :LABEL :TYPE :END_ID :START_ID} column)
      column
      (name column))
    (name column)))

(defn write-csv
  ([final-columns path data] (write-csv true \tab final-columns path data))
  ([sep final-columns path data] (write-csv true sep final-columns path data))
  ([is-stage sep final-columns path data]
   (let [columns final-columns
         headers (map #(create-header % is-stage) final-columns)
         rows (mapv #(mapv % columns) data)]
     (with-open [f (io/writer path)]
       (csv/write-csv f (cons headers rows) :separator sep)))))

(defn csv->map
  [csv-data]
  (map
   zipmap (->>
           (first csv-data)
           (map keyword)
           repeat)
   (rest csv-data)))

(defn inner-join
  [left-keys right-keys]
  (intersection (set left-keys) (set right-keys)))

(defn outer-join
  [left-keys right-keys]
  (union (difference (set left-keys) (set right-keys)) (difference (set right-keys) (set left-keys))))

(defn full-join
  [left-keys right-keys]
  (union (set left-keys) (set right-keys)))

(defn left-join
  [left-keys right-keys]
  left-keys)

(defn map-combine [group1 group2 dumy-map]
  (for [map1 group1
        map2 group2]
    (merge-with #(or %2 %1) dumy-map map2 map1)))

(defn joiner [left-coll right-coll left-fn right-fn join-type]
  (let [left-idx (group-by left-fn left-coll)
        right-idx (group-by right-fn right-coll)
        join-keys (set (join-type (keys left-idx) (keys right-idx)))]
    (apply concat
           (map #(map-combine (get left-idx  % [{}])
                              (get right-idx % [{}])
                              (zipmap (set (union (keys (first left-coll)) (keys (first right-coll)))) (repeat nil))) join-keys))))

(defn correct-source-id
  [dbXref]
  (let [processed-dbXref (
    cond
      (or (str/includes? dbXref "DOID") 
          (str/includes? dbXref "EFO")
          (str/includes? dbXref "HP")
          (str/includes? dbXref "KEGG")
          (str/includes? dbXref "MONDO")) (str/replace dbXref #":" "_")
      (or (str/includes? dbXref "ICD")
          (str/includes? (str/lower-case dbXref) "mesh")
          (str/includes? dbXref "MSH")
          (str/includes? dbXref "UMLS")
          (str/includes? (str/lower-case dbXref) "snomedct")
          (str/includes? dbXref "SCTID")
          (str/includes? dbXref "NCI")
          (str/includes? dbXref "MedDRA")) (str/replace dbXref #".*:" "")
      (str/includes? (str/lower-case dbXref) "orphanet") (str/replace (str/lower-case dbXref) #"orphanet*[_|:]" "ORPHA:")
      :else dbXref)]
  (cond 
    (or (str/includes? processed-dbXref "*") (str/includes? processed-dbXref "+")) (str/replace processed-dbXref #"[\*|\+]" "")
    (str/includes? processed-dbXref ".")(str/replace processed-dbXref "." "")
    :else processed-dbXref)))

(defn create-source
  [dbXref source]
  (cond
    (str/includes? dbXref "DOID") "DOID"
    (str/includes? dbXref "EFO") "EFO"
    (or (str/includes? dbXref "HPO")
        (str/includes? dbXref "HP")) "HPO"
    (str/includes? dbXref "MONDO") "MONDO"
    (str/includes? dbXref "ICD9") "ICD9CM"
    (or (str/includes? dbXref "ICD10") 
        (str/includes? dbXref "ICD-10")) "ICD10CM"
    (or (str/includes? dbXref "MSH") 
        (str/includes? (str/lower-case dbXref) "mesh")) "MESH"
    (str/includes? dbXref "UMLS") "UMLS"
    (or (str/includes? dbXref "SNOMEDCT")
        (str/includes? dbXref "SCTID")) "SNOMEDCT"
    (str/includes? dbXref "NCI") "NCIT"
    (str/includes? (str/lower-case dbXref) "orpha") "ORPHANET"
    (or (str/includes? (str/lower-case dbXref) "meddra")
        (str/includes? dbXref "MDR")) "MEDDRA"
    :else source
  ))
