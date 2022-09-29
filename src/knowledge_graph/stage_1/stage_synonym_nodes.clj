(ns knowledge-graph.stage-1.stage-synonym-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [camel-snake-kebab.core :as csk]
   [knowledge-graph.module.module :as kg]))

(defn get-synonyms
  ([file-path source] (get-synonyms file-path :synonym :label source))
  ([file-path key-synonym key-name source]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)
          data-map (kg/csv->map data)
          synonym (->> (map #(assoc % :label "SYNONYM") data-map)
                       (map #(assoc % :name (key-synonym %)))
                       (map #(assoc % :source source))
                       (mapv #(select-keys % [:label :name :source]))
                       distinct)
          name (->> (map #(assoc % :label "SYNONYM") data-map)
                    (map #(assoc % :name (key-name %)))
                    (map #(assoc % :source source))
                    (mapv #(select-keys % [:label :name :source]))
                    distinct)] 
      (concat synonym name)))))


(defn run []
  (let [doid (get-synonyms "stage_0_outputs/doid.csv" "DOID")
        efo (get-synonyms "stage_0_outputs/efo.csv" "EFO")
        hpo (get-synonyms "stage_0_outputs/hpo.csv" "HPO")
        icd9 (get-synonyms "stage_0_outputs/icd9.csv" :long_label :short_label "ICD9CM")
        icd10 (get-synonyms "stage_0_outputs/icd10.csv" "ICD10CM")
        medgen (get-synonyms "stage_0_outputs/medgen_id_mapping.csv" :synonym :medgen_name "MEDGEN")
        desc (get-synonyms "stage_0_outputs/mesh_descriptor.csv" "MESH")
        scr (get-synonyms "stage_0_outputs/mesh_scr.csv" "MESH")
        mondo (get-synonyms "stage_0_outputs/mondo.csv" "MONDO")
        ncit (get-synonyms "stage_0_outputs/ncit_meddra_mapping.csv" :synonym :ncit_name "NCIT")
        ncit_neo (get-synonyms "stage_0_outputs/ncit_neoplasm_mapping.csv" :synonym :ncit_name "NCIT")
        orphanet (get-synonyms "stage_0_outputs/orphanet.csv" "ORPHANET")
        snomedct (get-synonyms "stage_0_outputs/snomedct_icd10.csv" :synonym :snomed_name "SNOMEDCT")
        umls (get-synonyms "stage_0_outputs/umls.csv" "UMLS")
        synonyms (concat doid efo hpo icd9 icd10 medgen desc scr mondo ncit ncit_neo orphanet snomedct umls)] 
    (->>
      (distinct synonyms)
      (filter #(not= (:name %) ""))
      (filter #(some? (:name %)))
      (map #(assoc % :fake_id (hash (str/join "_" [(:name %) (:source %)]))))
      (map #(assoc % :snake_case_name (csk/->snake_case_string (:name %))))
      (map #(assoc % :id (str/join "_" [(:fake_id %) (:snake_case_name %) (:source %)])))
      (kg/write-csv [:label :id :name :source] "./resources/stage_1_outputs/synonym_nodes.csv"))))