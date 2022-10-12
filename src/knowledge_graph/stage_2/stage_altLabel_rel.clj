(ns knowledge-graph.stage-2.stage-altLabel-rel
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.set :as set]
    [clojure.data.csv :as csv]
    [knowledge-graph.module.module :as kg]))


(defn get-synonyms-from-ontology
  ([file-path] (get-synonyms-from-ontology file-path :synonym))
  ([file-path synonym]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)]
      (->> (kg/csv->map data)
           (filter #(not (str/blank? (:id %))))
           (map #(assoc % :start (cond
              (contains? % :source_id) (:source_id %)
              :else (:id %))))
           (map #(set/rename-keys % {synonym  :end}))
           (mapv #(select-keys % [:start :end]))
           distinct)))))

(defn get-nodes
  [node-file-path]
  (with-open [file (io/reader (io/resource node-file-path))]
    (let [node (csv/read-csv file :separator \tab)]
      (->> (kg/csv->map node)
           (mapv #(select-keys % [:id :name :source_id]))))))

(defn run 
  [_]
  (let[synonym-nodes (->> (get-nodes "stage_1_outputs/synonym_nodes.csv")
                          (map #(set/rename-keys % {:id :end_id}))
                          (mapv #(select-keys % [:end_id :name])))
       disease-nodes (->> (get-nodes "stage_1_outputs/disease_nodes.csv")
                          (map #(set/rename-keys % {:id :start_id}))
                          (mapv #(select-keys % [:start_id :source_id])))
       doid_synonym (get-synonyms-from-ontology "stage_0_outputs/doid.csv")
       efo_synonym (get-synonyms-from-ontology "stage_0_outputs/efo.csv")
       hpo_synonym (get-synonyms-from-ontology "stage_0_outputs/hpo.csv")
       mondo_synonym (get-synonyms-from-ontology "stage_0_outputs/mondo.csv")
       orphanet_synonym (get-synonyms-from-ontology "stage_0_outputs/orphanet.csv")
       mesh_desc_synonym (get-synonyms-from-ontology "stage_0_outputs/mesh_descriptor.csv")
       mesh_scr_synonym (get-synonyms-from-ontology "stage_0_outputs/mesh_scr.csv")
       medgen_synonym (get-synonyms-from-ontology "stage_0_outputs/medgen.csv")
       ncit_synonym (get-synonyms-from-ontology "stage_0_outputs/ncit.csv")
       ncit_synonym_from_mapping (get-synonyms-from-ontology "stage_0_outputs/ncit_mapping.csv")
       icd9_synonym (get-synonyms-from-ontology "stage_0_outputs/icd9.csv") 
       snomedct_synonym (get-synonyms-from-ontology "stage_0_outputs/snomedct_mapping.csv") 
       umls_synonym (get-synonyms-from-ontology "stage_0_outputs/umls.csv")
       synonyms (->> (concat doid_synonym efo_synonym hpo_synonym mondo_synonym
                             orphanet_synonym mesh_scr_synonym mesh_desc_synonym
                             medgen_synonym ncit_synonym ncit_synonym_from_mapping
                             snomedct_synonym icd9_synonym umls_synonym)
                     distinct)
       altLabel (-> (kg/joiner disease-nodes synonyms :source_id :start kg/inner-join)
                    (kg/joiner synonym-nodes :end :name kg/inner-join))] 
      (->> (map #(assoc % :type "altLabel") altLabel) 
           (map #(select-keys % [:start_id :type :end_id]))
           distinct
           (kg/write-csv [:start_id :type :end_id] "./resources/stage_2_outputs/altLabel_rel.csv"))))
