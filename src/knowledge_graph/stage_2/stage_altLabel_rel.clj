(ns knowledge-graph.stage-2.stage-altLabel-rel
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-synonyms-from-ontology
  [file-path source]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)]
      (->> (kg/csv->map data)
           (filter #(not (str/blank? (:id %))))
           (map #(assoc % :start (:source_id %)))
           (map #(assoc % :end_source source))
           (map #(set/rename-keys % {:synonym  :end}))
           (filter #(not (str/blank? (:end %))))
           (mapv #(select-keys % [:start :end :end_source]))
           distinct))))

(defn get-nodes
  [node-file-path]
  (with-open [file (io/reader (io/resource node-file-path))]
    (let [node (csv/read-csv file :separator \tab)]
      (->> (kg/csv->map node)
           (mapv #(select-keys % [:id :name :source_id :source]))))))

(defn run
  [_]
  (let [synonym-nodes (->> (get-nodes "stage_1_outputs/synonym_nodes.csv")
                           (map #(set/rename-keys % {:id :end_id}))
                           (mapv #(select-keys % [:end_id :name :source])))
        disease-nodes (->> (get-nodes "stage_1_outputs/disease_nodes.csv")
                           (map #(set/rename-keys % {:id :start_id :source :start_source}))
                           (mapv #(select-keys % [:start_id :source_id :start_source])))
        doid_synonym (get-synonyms-from-ontology "stage_0_outputs/doid.csv" "DOID")
        efo_synonym (get-synonyms-from-ontology "stage_0_outputs/efo.csv" "EFO")
        hpo_synonym (get-synonyms-from-ontology "stage_0_outputs/hpo.csv" "HPO")
        mondo_synonym (get-synonyms-from-ontology "stage_0_outputs/mondo.csv" "MONDO")
        orphanet_synonym (get-synonyms-from-ontology "stage_0_outputs/orphanet.csv" "ORPHANET")
        mesh_desc_synonym (get-synonyms-from-ontology "stage_0_outputs/mesh_des.csv" "MESH")
        mesh_scr_synonym (get-synonyms-from-ontology "stage_0_outputs/mesh_scr.csv" "MESH")
        medgen_synonym (get-synonyms-from-ontology "stage_0_outputs/medgen.csv" "MEDGEN")
        meddra_synonym (get-synonyms-from-ontology "stage_0_outputs/meddra.csv" "MEDDRA")
        ncit_synonym (get-synonyms-from-ontology "stage_0_outputs/ncit.csv" "NCIT")
        icdo_synonym (get-synonyms-from-ontology "stage_0_outputs/icdo.csv" "ICDO-3")
        icd9_synonym (get-synonyms-from-ontology "stage_0_outputs/icd9.csv" "ICD9CM")
        icd10_synonym (get-synonyms-from-ontology "stage_0_outputs/icd10.csv" "ICD10CM")
        icd11_synonym (get-synonyms-from-ontology "stage_0_outputs/icd11.csv" "ICD11")
        snomedct_synonym (get-synonyms-from-ontology "stage_0_outputs/snomedct.csv" "SNOMEDCT")
        umls_synonym (get-synonyms-from-ontology "stage_0_outputs/umls.csv" "UMLS")
        kegg_synonym (get-synonyms-from-ontology "stage_0_outputs/kegg.csv" "KEGG")
        phecode_synonym (get-synonyms-from-ontology "stage_0_outputs/phecode.csv" "PHECODE")
        synonyms (->> (concat doid_synonym efo_synonym hpo_synonym mondo_synonym
                              orphanet_synonym mesh_scr_synonym mesh_desc_synonym
                              medgen_synonym ncit_synonym snomedct_synonym, meddra_synonym
                              icd9_synonym umls_synonym kegg_synonym icdo_synonym
                              icd11_synonym icd10_synonym phecode_synonym)
                      distinct)
        altLabel (-> (kg/joiner synonyms synonym-nodes :end :name kg/inner-join)
                     (kg/joiner disease-nodes :start :source_id kg/inner-join))]
    (->>  (filter #(= (:end_source %) (:start_source %)) altLabel)
          (map #(assoc % :type "altLabel"))
          (map #(select-keys % [:start_id :type :end_id]))
          distinct
          (kg/write-csv [:start_id :type :end_id] "./resources/stage_2_outputs/altLabel_rel.csv"))))
