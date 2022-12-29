(ns knowledge-graph.stage-2.stage-hasDbXref-rel
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn hasDbXref
  [file-path source_label]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         (kg/csv->map)
         (filter #(not (nil? (:hasDbXref %))))
         (filter #(not (str/blank? (:hasDbXref %))))
         (map #(assoc % :end_source (:dbXref_source %)))
         (filter #(or
                   (= (:end_source %) "DOID") (= (:end_source %) "EFO") (= (:end_source %) "HPO")
                   (= (:end_source %) "ICDO-3") (= (:end_source %) "ICD9CM") (= (:end_source %) "ICD10CM")
                   (= (:end_source %) "ICD11") (= (:end_source %) "KEGG") (= (:end_source %) "MEDGEN")
                   (= (:end_source %) "MONDO") (= (:end_source %) "NCIT") (= (:end_source %) "ORPHANET")
                   (= (:end_source %) "SNOMEDCT") (= (:end_source %) "UMLS") (= (:end_source %) "MESH")))
         (map #(assoc % :start_source source_label))
         (map #(set/rename-keys % {:hasDbXref :end :source_id :start}))
         (mapv #(select-keys % [:start :start_source :end :end_source]))
         (filter #(not (str/blank? (:start %))))
         distinct)))

(defn disease-nodes
  [disease-file-path]
  (with-open [d-file (io/reader (io/resource disease-file-path))]
    (->> (csv/read-csv d-file :separator \tab)
         (kg/csv->map)
         (mapv #(select-keys % [:id :source_id :source]))
         distinct)))

(def doid-dbXref (hasDbXref "stage_0_outputs/doid.csv" "DOID"))
(def efo-dbXref (hasDbXref "stage_0_outputs/efo.csv" "EFO"))
(def hpo-dbXref (hasDbXref "stage_0_outputs/hpo.csv" "HPO"))
(def icdo-dbXref (hasDbXref "stage_0_outputs/icdo.csv" "ICDO-3"))
(def icd9-dbXref (hasDbXref "stage_0_outputs/icd9.csv" "ICD9CM"))
(def icd10-dbXref (hasDbXref "stage_0_outputs/icd10.csv" "ICD10CM"))
(def icd11-dbXref (hasDbXref "stage_0_outputs/icd11.csv" "ICD11"))
(def kegg-dbXref (hasDbXref "stage_0_outputs/kegg.csv" "KEGG"))
(def medgen-dbXref (hasDbXref "stage_0_outputs/medgen.csv" "MEDGEN"))
(def meddra-dbXref (hasDbXref "stage_0_outputs/meddra.csv" "MEDDRA"))
(def mondo-dbXref (hasDbXref "stage_0_outputs/mondo.csv" "MONDO"))
(def ncit-dbXref (hasDbXref "stage_0_outputs/ncit.csv" "NCIT"))
(def orphanet-dbXref (hasDbXref "stage_0_outputs/orphanet.csv" "ORPHANET"))
(def snomedct-dbXref (hasDbXref "stage_0_outputs/snomedct.csv" "SNOMEDCT"))
(def umls-dbXref (hasDbXref "stage_0_outputs/umls.csv" "UMLS"))
(def phecode-dbXref (hasDbXref "stage_0_outputs/phecode.csv" "PHECODE"))
(def disease (disease-nodes "stage_1_outputs/disease_nodes.csv"))

(defn run [_]
  (let [dbXref (distinct (concat doid-dbXref efo-dbXref
                                 hpo-dbXref mondo-dbXref
                                 orphanet-dbXref kegg-dbXref
                                 umls-dbXref icd10-dbXref
                                 icdo-dbXref icd9-dbXref
                                 medgen-dbXref ncit-dbXref
                                 icd11-dbXref snomedct-dbXref
                                 phecode-dbXref meddra-dbXref))
        disease-dbXref-end (->> (kg/joiner dbXref
                                           (map #(set/rename-keys % {:id :end_id :source_id :end_mapping_id}) disease)
                                           :end :end_mapping_id
                                           kg/left-join)
                                (filter #(not (nil? (:end_mapping_id %))))
                                (filter #(= (:end_source %) (:source %)))
                                (map #(select-keys % [:end_id :start :start_source])))
        disease-dbXref (->> (kg/joiner disease-dbXref-end
                                       (map #(set/rename-keys % {:id :start_id :source_id :start_mapping_id}) disease)
                                       :start :start_mapping_id
                                       kg/left-join)
                            (filter #(not (nil? (:start_mapping_id %))))
                            (filter #(= (:start_source %) (:source %)))
                            (map #(select-keys % [:start_id :end_id])))
        reversed-disease-dbXref (map #(set/rename-keys % {:start_id :end_id :end_id :start_id}) disease-dbXref)]
    (->> (concat disease-dbXref reversed-disease-dbXref)
         (filter #(not= (:start_id %) (:end_id %)))
         (map #(assoc % :type "hasDbXref"))
         distinct
         (kg/write-csv [:start_id :type :end_id] "./resources/stage_2_outputs/hasDbXref_rel.csv"))))