(ns knowledge-graph.stage-1.stage-disease-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn load-disease
  [file-path source]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)]
      (->> (kg/csv->map data)
           (map #(assoc % :source source))
           (mapv #(select-keys % [:id :label :name :source_id :source]))))))


(defn run []
  (let [doid (load-disease "stage_1_outputs/doid_nodes.csv") 
        efo (load-disease "stage_1_outputs/efo_nodes.csv")
        hpo (load-disease "stage_1_outputs/hpo_nodes.csv")
        icd9 (load-disease "stage_1_outputs/icd9_nodes.csv")
        icd10 (load-disease "stage_1_outputs/icd10_nodes.csv")
        meddra (load-disease "stage_1_outputs/meddra_nodes.csv")
        medgen (load-disease "stage_1_outputs/medgen_nodes.csv")
        mesh (load-disease "stage_1_outputs/mesh_nodes.csv")
        mondo (load-disease "stage_1_outputs/mondo_nodes.csv")
        ncit (load-disease "stage_1_outputs/ncit_nodes.csv")
        orphanet (load-disease "stage_1_outputs/orphanet_nodes.csv")
        snomedct (load-disease "stage_1_outputs/snomedct_nodes.csv")
        umls (load-disease "stage_1_outputs/umls_nodes.csv")
        disease-nodes (concat doid efo hpo icd9 icd10 meddra medgen mesh mondo ncit orphanet snomedct umls)]
        (->>(distinct disease-nodes)
            (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/disease_nodes.csv"))))

