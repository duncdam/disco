(ns knowledge-graph.stage-0.get-icd9
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [knowledge-graph.module.module :as kg]))

(defn file->map
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->>
     (slurp file)
     str/split-lines
     (map #(str/split % #"\s{1,}"))
     (cons ["id" "label"])
     (map #(into [] [(first %) (str/join " " (rest %))]))
     kg/csv->map)))

(defn get-results
  [output-path]
  (let [icd9-long  (->> (file->map "icd9/icd9_long_desc.txt")
                        (map #(set/rename-keys % {:label :long_label})))
        icd9-short (->> (file->map "icd9/icd9_short_desc.txt")
                        (map #(set/rename-keys % {:label :short_label})))]

    (->>
     (kg/joiner icd9-long icd9-short :id :id kg/left-join)
     (filter #(some? (:id %)))
     (kg/write-csv [:id :short_label :long_label] output-path))))

(defn run
  []
  (let [output-path "./resources/stage_0_outputs/icd9.csv"]
    (get-results output-path)))
