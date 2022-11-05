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
          synonym (->> (map #(assoc % :name (key-synonym %)) data-map)
                       (map #(assoc % :label "SYNONYM"))
                       (mapv #(select-keys % [:label :name]))
                       distinct)
          name (->> (map #(assoc % :name (key-name %)) data-map)
                    (map #(assoc % :label "SYNONYM"))
                    (mapv #(select-keys % [:label :name]))
                    distinct)] 
      (concat synonym name)))))


(defn run [_]
  (let [doid (get-synonyms "stage_0_outputs/doid.csv")
        efo (get-synonyms "stage_0_outputs/efo.csv")
        hpo (get-synonyms "stage_0_outputs/hpo.csv")
        icd9 (get-synonyms "stage_0_outputs/icd9.csv")
        icd10 (get-synonyms "stage_0_outputs/icd10.csv")
        meddra (get-synonyms "stage_0_outputs/meddra.csv")
        medgen (get-synonyms "stage_0_outputs/medgen.csv")
        mondo (get-synonyms "stage_0_outputs/mondo.csv")
        desc (get-synonyms "stage_0_outputs/mesh_descriptor.csv")
        scr (get-synonyms "stage_0_outputs/mesh_scr.csv")
        ncit (get-synonyms "stage_0_outputs/ncit.csv")
        orphanet (get-synonyms "stage_0_outputs/orphanet.csv")
        snomedct (get-synonyms "stage_0_outputs/snomedct.csv")
        umls (get-synonyms "stage_0_outputs/umls.csv")
        kegg (get-synonyms "stage_0_outputs/kegg.csv")
        icd11 (get-synonyms "stage_0_outputs/icd11.csv")
        synonyms (concat doid efo hpo icd9 icd10 medgen desc meddra scr mondo ncit orphanet snomedct umls kegg icd11)] 
    (->> (filter #(not= (:name %) "") synonyms)
         (filter #(some? (:name %)))
         (map #(assoc % :fake_id (hash (:name %))))
         (map #(assoc % :con_name (str/replace (:name %) " " "_")))
         (map #(assoc % :id (str/join "_" [(:fake_id %) (:con_name %)])))
         distinct
         (kg/write-csv [:label :id :name] "./resources/stage_1_outputs/synonym_nodes.csv"))))