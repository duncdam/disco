(ns knowledge-graph.stage-2.stage-altLabel-rel
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.set :as set]
    [clojure.data.csv :as csv]
    [knowledge-graph.module.module :as kg]))

(defn process-id
  [id]
  (cond
    (str/includes? id "/") (str/replace (last (str/split id #"/")) "_" ":")
    (str/includes? id "_") (str/replace id "_" ":")
    :else id))

(defn get-synonyms-from-ontology
  ([file-path id] (get-synonyms-from-ontology file-path id :synonym))
  ([file-path id synonym]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (csv/read-csv file :separator \tab)]
      (->> (kg/csv->map data)
           (filter #(some? (id %)))
           (filter #(not= (id %) ""))
           (filter #(not= (:label_type %) "PN"))
           (map #(assoc % :start (process-id (id %)))) 
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
  []
  (let[synonym-nodes (->> (get-nodes "stage_1_outputs/synonym_nodes.csv")
                          (map #(set/rename-keys % {:id :end_id}))
                          (mapv #(select-keys % [:end_id :name])))
       disease-nodes (->> (get-nodes "stage_1_outputs/disease_nodes.csv")
                          (map #(set/rename-keys % {:id :start_id}))
                          (mapv #(select-keys % [:start_id :source_id])))
       doid_synonym (get-synonyms-from-ontology "stage_0_outputs/doid.csv" :id)
       efo_synonym (get-synonyms-from-ontology "stage_0_outputs/efo.csv" :id)
       hpo_synonym (get-synonyms-from-ontology "stage_0_outputs/hpo.csv" :id)
       mondo_synonym (get-synonyms-from-ontology "stage_0_outputs/mondo.csv" :id)
       orphanet_synonym (->> (get-synonyms-from-ontology "stage_0_outputs/orphanet.csv" :source_id))
       mesh_desc_synonym (get-synonyms-from-ontology "stage_0_outputs/mesh_descriptor.csv" :id)
       mesh_scr_synonym (get-synonyms-from-ontology "stage_0_outputs/mesh_scr.csv" :id)
       icd9_synonym (get-synonyms-from-ontology "stage_0_outputs/icd9.csv" :id :short_label) 
       snomedct_synonym (get-synonyms-from-ontology "stage_0_outputs/snomedct_icd10.csv" :snomed_id) 
       umls_synonym (get-synonyms-from-ontology "stage_0_outputs/umls.csv" :cuid :label)
       synonyms (->> (concat doid_synonym efo_synonym hpo_synonym mondo_synonym
                             orphanet_synonym mesh_scr_synonym mesh_desc_synonym
                             snomedct_synonym icd9_synonym umls_synonym)
                     distinct)
       altLabel (-> (kg/joiner synonyms synonym-nodes :end :name kg/inner-join)
                    (kg/joiner disease-nodes :start :source_id kg/inner-join))] 
      (->> (map #(assoc % :type "altLabel") altLabel) 
           (mapv #(select-keys % [:start_id :type :end_id]))
           distinct
           (kg/write-csv [:start_id :type :end_id] "./resources/stage_2_outputs/altLabel_rel.csv"))))
