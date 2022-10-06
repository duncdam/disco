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
  [long-file-path short-file-path output-path]
  (let [icd9-long  (->> (file->map long-file-path)
                        (map #(set/rename-keys % {:label :long_label})))
        icd9-short (->> (file->map short-file-path)
                        (map #(set/rename-keys % {:label :short_label})))]

    (->>
     (kg/joiner icd9-long icd9-short :id :id kg/left-join)
     (filter #(some? (:id %)))
     (map #(set/rename-keys % {:long_label :label :short_label :synonym}))
     (kg/write-csv [:id :label :synonym] output-path))))

(defn run
  [_]
  (let [output-path "./resources/stage_0_outputs/icd9.csv"
        long-file-path "download/CMS32_DESC_LONG_DX.txt"
        short-file-path "download/CMS32_DESC_SHORT_DX.txt"]
    (get-results long-file-path short-file-path output-path)))
