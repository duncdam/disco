(ns knowledge-graph.module.module
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :refer [difference intersection union]]))

;; I stole this from https://stackoverflow.com/questions/47333668/split-lines-in-clojure-while-reading-from-file
(defn lines-reducible [rdr]
  (reify clojure.lang.IReduceInit
    (reduce [this f init]
      (try
        (loop [state init]
          (if (reduced? state)
            @state
            (if-let [line (.readLine rdr)]
              (recur (f state line))
              state)))
        (finally
          (.close rdr))))))

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
         rows (map #(map % columns) data)]
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

;; found this sql join in clojure here: https://gist.github.com/Chort409/eb46f4d95261d9af51e9
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

(defn correct-source
  [dbXref]
  (cond
    (str/includes? dbXref "DOID") "DOID"
    (str/includes? dbXref "EFO") "EFO"
    (or (str/includes? dbXref "HPO")
        (str/includes? dbXref "HP")) "HPO"
    (str/includes? dbXref "MONDO") "MONDO"
    (str/includes? dbXref "ICD9") "ICD9CM"
    (or (str/includes? dbXref "ICDO")
        (str/includes? dbXref "ICD-O")) "ICDO-3"
    (or (str/includes? dbXref "ICD10") 
        (str/includes? dbXref "ICD-10")) "ICD10CM"
    (or (str/includes? dbXref "ICD11") 
       (str/includes? dbXref "ICD-11")) "ICD11"
    (or (str/includes? dbXref "MSH") 
        (str/includes? (str/lower-case dbXref) "mesh")) "MESH"
    (str/includes? dbXref "UMLS") "UMLS"
    (str/includes? dbXref "KEGG") "KEGG"
    (or (str/includes? dbXref "SNOMEDCT")
        (str/includes? dbXref "SCTID")) "SNOMEDCT"
    (str/includes? dbXref "NCI") "NCIT"
    (str/includes? (str/lower-case dbXref) "orpha") "ORPHANET"
    (or (str/includes? (str/lower-case dbXref) "meddra")
        (str/includes? dbXref "MDR")) "MEDDRA"
    (nil? dbXref) ""
    :else dbXref
  ))

(defn correct-xref-id
  [dbXref]
  (let [processed-dbXref (
    cond
      (nil? dbXref) ""
      (or (str/includes? dbXref "DOID") 
          (str/includes? dbXref "EFO")
          (str/includes? dbXref "HP")
          (str/includes? dbXref "MONDO")) dbXref 
      (str/includes? (str/lower-case dbXref) "orphanet") (str/replace (str/lower-case dbXref) #"orphanet*[_|:]" "ORPHA:")
      :else (last (str/split dbXref #":")))]
  (cond 
    (or (str/includes? processed-dbXref "*") (str/includes? processed-dbXref "+")) (str/replace processed-dbXref #"[\*|\+]" "")
    (str/includes? processed-dbXref ".")(str/replace processed-dbXref "." "")
    :else processed-dbXref)))