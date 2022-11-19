(ns knowledge-graph.stage-1.stage-disease-nodes
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn load-disease
  [file-path source]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)]
      (->> (kg/csv->map data)
           (map #(assoc % :source source))
           (map #(assoc % :name (:label %)))
           (map #(assoc % :label source))
           (map #(assoc % :id (cond
              (str/includes? (:id %) "_") (:id %)
              :else (str/join "_" [(:source %) (:id %)]))))
           (mapv #(select-keys % [:id :label :name :source_id :source]))
           distinct))))


(defn run [_]
  (let [doid (load-disease "stage_0_outputs/doid.csv" "DOID") 
        efo (load-disease "stage_0_outputs/efo.csv" "EFO")
        hpo (load-disease "stage_0_outputs/hpo.csv" "HPO")
        mondo (load-disease "stage_0_outputs/mondo.csv" "MONDO")
        icd9 (load-disease "stage_0_outputs/icd9.csv" "ICD9CM")
        icdo (load-disease "stage_0_outputs/icdo.csv" "ICDO-3")      
        icd10 (load-disease "stage_0_outputs/icd10.csv" "ICD10CM")
        icd11 (load-disease "stage_0_outputs/icd11.csv" "ICD11")
        meddra (load-disease "stage_0_outputs/meddra.csv" "MEDDRA")
        medgen (load-disease "stage_0_outputs/medgen.csv" "MEDGEN")
        mesh-des (load-disease "stage_0_outputs/mesh_des.csv" "MESH")
        mesh-scr (load-disease "stage_0_outputs/mesh_scr.csv" "MESH")
        ncit (load-disease "stage_0_outputs/ncit.csv" "NCIT")
        orphanet (load-disease "stage_0_outputs/orphanet.csv" "ORPHANET")
        snomedct (load-disease "stage_0_outputs/snomedct.csv" "SNOMEDCT")
        umls (load-disease "stage_0_outputs/umls.csv" "UMLS")
        kegg (load-disease "stage_0_outputs/kegg.csv" "KEGG")        
        disease-nodes (concat doid efo hpo mondo orphanet icdo
                              icd9 icd10 snomedct umls meddra
                              medgen mesh-des mesh-scr ncit kegg icd11)]
        (->> (distinct disease-nodes)
             (kg/write-csv [:id :label :name :source_id :source] "./resources/stage_1_outputs/disease_nodes.csv"))))

