(ns knowledge-graph.stage-0.get-snomedct-icd10-mapping
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [knowledge-graph.module.module :as kg]))

(defn create-snomed-core
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (->> (csv/read-csv file :separator \tab)
                    kg/csv->map)
          synonym_1 (->> (map #(select-keys % [:SCTID
                                               :Fully_Specified_Name
                                               :ICD_10_CM
                                               :Clinician_Friendly_Name]) data)
                         (map #(set/rename-keys % {:SCTID   :snomed_id
                                                   :Clinician_Friendly_Name :snomed_name
                                                   :ICD_10_CM              :icd10
                                                   :Fully_Specified_Name   :synonym})))
          synonym_2 (->> (map #(select-keys % [:SCTID
                                               :Clinician_Friendly_Name
                                               :ICD_10_CM
                                               :Patient_Friendly_Name]) data)
                         (map #(set/rename-keys % {:SCTID   :snomed_id
                                                   :Clinician_Friendly_Name :snomed_name
                                                   :ICD_10_CM              :icd10
                                                   :Patient_Friendly_Name   :synonym})))]
          (->> (concat synonym_1 synonym_2)
               (filter #(some? (:snomed_id %)))
               (mapv #(select-keys % [:snomed_id :snomed_name :icd10 :synonym]))))))
 
(defn create-snomed->icd10
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (->> (csv/read-csv file :separator \tab)
                    kg/csv->map
                    (map #(select-keys % [:referencedComponentId
                                          :referencedComponentName
                                          :mapTarget
                                          :mapTargetName]))
                    (map #(set/rename-keys % {:referencedComponentId   :snomed_id
                                              :referencedComponentName :snomed_name
                                              :mapTarget              :icd10
                                              :mapTargetName          :synonym})))]
          (->> (filter #(some? (:snomed_id %)) data)
               (mapv #(select-keys % [:snomed_id :snomed_name :icd10 :synonym]))))))



(defn run
  []
  (let [snomed->icd10-file "snomedct/snomedct_icd10.tsv"
        snomed-core-file "snomedct/snomedct_mapset.tsv"
        output-path "./resources/stage_0_outputs/snomedct_icd10.csv"
        snomed->icd10 (create-snomed->icd10 snomed->icd10-file)
        snomed-core (create-snomed-core snomed-core-file)]
    (->> (concat snomed->icd10 snomed-core)
         distinct
         (kg/write-csv [:snomed_id :snomed_name :icd10 :synonym] output-path))))
