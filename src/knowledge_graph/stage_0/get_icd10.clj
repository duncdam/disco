(ns knowledge-graph.stage-0.get-icd10
  (:require
   [clj-http.client :as client]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [url output-path]
  (with-open [body (:body (client/get url {:as :reader}))]
    (->>
     (slurp body)
     str/split-lines
     (map #(str/split % #"\s{1,}"))
     (cons ["id" "label"])
     (map #(into [] [(first %) (str/join " " (rest %))]))
     kg/csv->map
     (filter #(some? (:id %)))
     (kg/write-csv [:id :label :source] output-path))))

(defn run [_]
  (let [url "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2022/icd10cm_codes_2022.txt"
        output-path "./resources/stage_0_outputs/icd10.csv"]
    (get-results url output-path)))