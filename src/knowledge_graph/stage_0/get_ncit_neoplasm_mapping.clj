(ns knowledge-graph.stage-0.get-ncit-neoplasm-mapping
  (:require
   [camel-snake-kebab.core :as csk]
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.set :as set]
   [knowledge-graph.module.module :as kg]))

(defn csv->map
  [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map #(csk/->camelCase %))
            (map keyword)
            repeat)
       (rest csv-data)))

(defn get-results
  [url output-path]
  (->> (client/get url {:as :reader})
       :body
       csv/read-csv
       csv->map
       (map #(select-keys % [:ncItCode
                             :ncItPreferredTerm
                             :ncImCui
                             :ncImPreferredName
                             :ncImSource
                             :sourceCode
                             :sourceTerm]))
       (map #(set/rename-keys % {:ncItCode          :ncit_id
                                 :ncItPreferredTerm :ncit_name
                                 :ncImCui           :ncim_cui
                                 :ncImPreferredName :ncim_name
                                 :ncImSource        :ncim_source
                                 :sourceCode        :source_code
                                 :sourceTerm        :source_name}))
       (filter #(some? (:ncit_id %)))
       (kg/write-csv [:ncit_id
                      :ncit_name
                      :ncim_cui
                      :ncim_name
                      :ncim_source
                      :source_code
                      :source_name] output-path)))

(defn run
  []
  (let [url "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Neoplasm/Neoplasm_Core_Mappings_NCIm_Terms.csv"
        output-path "./resources/stage_0_outputs/ncit_neoplasm_mapping.csv"]
    (get-results url output-path)))