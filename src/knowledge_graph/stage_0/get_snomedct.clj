(ns knowledge-graph.stage-0.get-snomedct
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-snomedct
  [snomed-file-path]
  (with-open [file (io/reader (io/resource snomed-file-path))]
    (->> (csv/read-csv file :separator \|)
         (kg/csv->map)
         (filter #(= (:SNOMED_CONCEPT_STATUS %) "Current"))
         (map #(set/rename-keys % {:SNOMED_CID :id :SNOMED_FSN :label}))
         (map #(assoc % :hasDbXref (:UMLS_CUI %)))
         (map #(assoc % :dbXref_source "UMLS"))
         (map #(assoc % :source_id (:id %)))
         (mapv #(select-keys % [:id :label :source_id :hasDbXref :dbXref_source]))
         distinct)))

(defn get-snomedct-orphanet
  [snomed-orphanet-mapping-file-path snomed-file-path]
  (let [orphanet-mapping (->> (-> (io/resource snomed-orphanet-mapping-file-path)
                                  io/reader
                                  (csv/read-csv :separator \tab))
                              kg/csv->map
                              (map #(select-keys % [:referencedComponentId :mapTarget]))
                              (map #(set/rename-keys % {:referencedComponentId :id :mapTarget :hasDbXref}))
                              (map #(assoc % :hasDbXref (str/join ":" ["ORPHA" (:hasDbXref %)])))
                              (map #(assoc % :dbXref_source "ORPHANET")))
        snomed (->> (get-snomedct snomed-file-path)
                    (map #(select-keys % [:id :label :source_id])))
        snomed-orphanet (kg/joiner snomed orphanet-mapping :id :id kg/inner-join)]
    snomed-orphanet))

(def snomed-file-path "downloads/SNOMEDCT_CORE_SUBSET_202211.txt")
(def snomed-orphanet-mapping-file-path "downloads/SnomedCT_SNOMEDOrphanetMapPackage_PRODUCTION_20211031T120000Z/Full/Refset/Map/der2_sRefset_OrphanetSimpleMapFull_INT_20210731.txt")
(def output-path "./resources/stage_0_outputs/snomedct.csv")

(defn run [_]
  (let [snomedct (get-snomedct snomed-file-path)
        snomedct-orphanet (get-snomedct-orphanet snomed-orphanet-mapping-file-path snomed-file-path)]
    (->> (concat snomedct snomedct-orphanet)
         (map #(assoc % :subClassOf ""))
         (map #(assoc % :synonym ""))
         (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path))))