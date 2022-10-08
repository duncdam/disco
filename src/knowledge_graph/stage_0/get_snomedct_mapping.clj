(ns knowledge-graph.stage-0.get-snomedct-mapping
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-snomed-mapping
  [core-file-path icd10-file-path]
  (with-open [core-file (io/reader (io/resource core-file-path))
              icd10-file (io/reader (io/resource icd10-file-path))]
    (let [data-core (->> (csv/read-csv core-file :separator \tab)
                         kg/csv->map
                         (map #(select-keys % [:SCTID :Fully_Specified_Name :ICD_10_CM :Clinician_Friendly_Name :Patient_Friendly_Name])))
          data-icd10 (->> (csv/read-csv icd10-file :separator \tab)
                          kg/csv->map
                          (map #(select-keys % [:referencedComponentId :referencedComponentName :mapTarget :mapTargetName])))
          synonym_1 (->> (map #(set/rename-keys % {:SCTID :id :ICD_10_CM :hasDbXref :Fully_Specified_Name :synonym}) data-core))
          synonym_2 (->> (map #(set/rename-keys % {:SCTID :id :ICD_10_CM :hasDbXref :Clinician_Friendly_Name :synonym}) data-core))
          synonym_3 (->> (map #(set/rename-keys % {:SCTID :id :ICD_10_CM :hasDbXref :Patient_Friendly_Name :synonym}) data-core))
          synonym_4 (->> (map #(set/rename-keys % {:referencedComponentId :id :mapTarget :hasDbXref :mapTargetName :synonym}) data-icd10))
          synonym_5 (->> (map #(set/rename-keys % {:referencedComponentId :id :mapTarget :hasDbXref :referencedComponentName :synonym}) data-icd10))]
          (->> (concat synonym_1 synonym_2 synonym_3 synonym_4 synonym_5)
               (map #(assoc % :dbXref_source "ICD10CM"))
               (map #(assoc % :hasDbXref (str/replace (:hasDbXref %) "." "")))
               (filter #(some? (:id %)))
               (mapv #(select-keys % [:id :hasDbXref :dbXref_source :synonym]))))))

(defn run
  [_]
  (let [snomed->icd10-file "downloads/SNOMED_CT_to_ICD-10-CM_Resources_20220901/tls_Icd10cmHumanReadableMap_US1000124_20220901.tsv"
        snomed-core-file "downloads/UMLS_KP_ProblemList_Mapping_20220301_Final.txt"
        output-path "./resources/stage_0_outputs/snomedct_mapping.csv"
        snomed-mapping (get-snomed-mapping snomed-core-file snomed->icd10-file)]
    (->> (distinct snomed-mapping)
         (kg/write-csv [:id :hasDbXref :dbXref_source :synonym] output-path))))
