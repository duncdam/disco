(ns knowledge-graph.module.module
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
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
