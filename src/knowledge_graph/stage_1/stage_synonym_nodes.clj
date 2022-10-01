(ns knowledge-graph.stage-1.stage-synonym-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-synonyms
  ([file-path] (get-synonyms file-path :synonym :label))
  ([file-path key-synonym key-name]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)
          data-map (kg/csv->map data)
          synonym (->> (map #(assoc % :label "SYNONYM") data-map)
                       (map #(assoc % :name (key-synonym %)))
                       (mapv #(select-keys % [:label :name]))
                       distinct)
          name (->> (map #(assoc % :label "SYNONYM") data-map)
                    (map #(assoc % :name (key-name %)))
                    (mapv #(select-keys % [:label :name]))
                    distinct)] 
      (concat synonym name)))))


(defn run []
  (let [doid (get-synonyms "stage_0_outputs/doid.csv")
        efo (get-synonyms "stage_0_outputs/efo.csv")
        hpo (get-synonyms "stage_0_outputs/hpo.csv")
        icd9 (get-synonyms "stage_0_outputs/icd9.csv" :long_label :short_label)
        icd10 (get-synonyms "stage_0_outputs/icd10.csv")
        medgen (get-synonyms "stage_0_outputs/medgen_id_mapping.csv" :synonym :medgen_name)
        desc (get-synonyms "stage_0_outputs/mesh_descriptor.csv")
        scr (get-synonyms "stage_0_outputs/mesh_scr.csv")
        mondo (get-synonyms "stage_0_outputs/mondo.csv")
        ncit (get-synonyms "stage_0_outputs/ncit_meddra_mapping.csv" :synonym :ncit_name)
        ncit_neo (get-synonyms "stage_0_outputs/ncit_neoplasm_mapping.csv" :synonym :ncit_name)
        orphanet (get-synonyms "stage_0_outputs/orphanet.csv")
        snomedct (get-synonyms "stage_0_outputs/snomedct_icd10.csv" :synonym :snomed_name)
        umls (get-synonyms "stage_0_outputs/umls.csv")
        synonyms (concat doid efo hpo icd9 icd10 medgen desc scr mondo ncit ncit_neo orphanet snomedct umls)] 
    (->>
      (distinct synonyms)
      (filter #(not= (:name %) ""))
      (filter #(some? (:name %)))
      (map #(assoc % :fake_id (hash (:name %))))
      (map #(assoc % :con_name (str/replace (:name %) " " "_")))
      (map #(assoc % :id (str/join "_" [(:fake_id %) (:con_name %)])))
      (kg/write-csv [:label :id :name] "./resources/stage_1_outputs/synonym_nodes.csv"))))