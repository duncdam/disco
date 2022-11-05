(ns knowledge-graph.stage-0.get-icd9
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
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
  [long-file-path short-file-path]
  (let [icd9-long  (->> (file->map long-file-path)
                        (map #(set/rename-keys % {:label :long_label})))
        icd9-short (->> (file->map short-file-path)
                        (map #(set/rename-keys % {:label :short_label})))]

    (->>
     (kg/joiner icd9-long icd9-short :id :id kg/left-join)
     (filter #(some? (:id %)))
     (map #(set/rename-keys % {:long_label :label :short_label :synonym}))
     (map #(assoc % :source_id (:id %)))
     (map #(assoc % :id (str/join "_" ["ICD9CM" (:id %)])))
     (map #(select-keys % [:id :label :source_id :synonym])))))

(defn get-result-mapping
  [icd10-url snomed-file-path]
  (let [icd9-icd10-map (->> (client/get icd10-url {:as :reader})
                            :body
                            csv/read-csv
                            kg/csv->map
                            (filter #(not= (:id9cm %) ""))
                            (map #(assoc % :dbXref_source "ICD10CM"))
                            (map #(set/rename-keys % {:icd9cm :id :icd10cm :hasDbXref }))
                            (mapv #(select-keys % [:id :hasDbXref :dbXref_source])))
        icd9-snomed-map (with-open [file (io/reader (io/resource snomed-file-path))]
                            (->>  (slurp file)
                                  str/split-lines
                                  (map #(str/split % #"\t"))
                                  (cons ["ICD_CODE" "ICD_NAME" "IS_CURRENT_ICD" "IP_USAGE" "OP_USAGE" "AVG_USAGE" "IS_NEC" "SNOMED_CID" "SNOMED_FSN" "IS_1-1MAP" "CORE_USAGE" "IN_CORE" ])
                                  kg/csv->map
                                  (filter #(not (str/includes? (:ICD_CODE %) "ICD_CODE")))
                                  (map #(assoc % :hasDbXref (:SNOMED_CID %)))
                                  (map #(assoc % :dbXref_source "SNOMED_CT"))
                                  (map #(assoc % :id (:ICD_CODE %)))
                                  (map #(assoc % :id (str/replace (:id %) "." "")))
                                  (mapv #(select-keys % [:id :hasDbXref :dbXref_source]))))
        icd9-mapping (->> (concat icd9-icd10-map icd9-snomed-map)
                          (map #(set/rename-keys % {:id :source_id}))
                          distinct)]
        (map #(select-keys % [:source_id :hasDbXref :dbXref_source]) icd9-mapping)))

(def output-path "./resources/stage_0_outputs/icd9.csv")
(def long-file-path "downloads/CMS32_DESC_LONG_DX.txt")
(def short-file-path "downloads/CMS32_DESC_SHORT_DX.txt")
(def icd10-url "https://data.nber.org/gem/icd9toicd10cmgem.csv")
(def snomed-file-path "downloads/ICD9CM_SNOMED_MAP_1TO1_202112.txt")

(defn run
  [_]
  (let [icd9-info (get-results long-file-path short-file-path)
        icd9-mapping (get-result-mapping icd10-url snomed-file-path)]
    (->> (kg/joiner icd9-info icd9-mapping :source_id :source_id kg/left-join) 
         (kg/write-csv [:id :label :source_id :synonym :hasDbXref :dbXref_source] output-path))))
