(ns knowledge-graph.stage-0.get-umls
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [file-path output-path]
  (with-open [f (io/reader (io/resource file-path))]
    (->>
     (csv/read-csv f :separator \tab)
     (kg/csv->map)
     ;; filter for disease and syndrom semantic types only
     (filter #(or 
        (str/includes? (:STY %) "Disease or Syndrome") 
        (str/includes? (:STY %) "Mental or Behavioral Dysfunction")
        (str/includes? (:STY %) "Neoplastic Process")))
     ;; remove all mouse diseases
     (filter #(not (str/includes? (str/lower-case (:STR %)) "mouse")))
     (map #(select-keys % [:CUI :STR :SAB :CODE :TTY]))
     (map #(set/rename-keys % {:CUI :cuid
                               :STR :label
                               :TTY :label_type
                               :SAB :ref_source
                               :CODE :ref_id}))
     (kg/write-csv [:cuid :label :label_type :ref_id :ref_source] output-path)
    )))

(defn run []
  (let [file-path "umls/umls_eng_raw.csv"
        output-path "./resources/stage_0_outputs/umls.csv"]
    (get-results file-path output-path)))