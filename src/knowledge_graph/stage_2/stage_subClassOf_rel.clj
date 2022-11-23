(ns knowledge-graph.stage-2.stage-subClassOf-rel
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-subClassOf-rel
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (map #(assoc % :start (:source_id %)))
         (map #(assoc % :end (:subClassOf %)))
         (filter #(not (str/blank? (:end %))))
         (mapv #(select-keys % [:start :end])))))

(defn disease-nodes
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (mapv #(select-keys % [:source_id :id])))))

(defn run
  [_]
  (let [doid-subClassOf (get-subClassOf-rel "stage_0_outputs/doid.csv")
        efo-subClassOf (get-subClassOf-rel "stage_0_outputs/efo.csv")
        hpo-subClassOf (get-subClassOf-rel "stage_0_outputs/hpo.csv")
        icd9-subClassOf (get-subClassOf-rel "stage_0_outputs/icd9.csv")
        icdo-subClassOf (get-subClassOf-rel "stage_0_outputs/icdo.csv")
        icd10-subClassOf (get-subClassOf-rel "stage_0_outputs/icd10.csv")
        icd11-subClassOf (get-subClassOf-rel "stage_0_outputs/icd11.csv")
        kegg-subClassOf (get-subClassOf-rel "stage_0_outputs/kegg.csv")
        meddra-subClassOf (get-subClassOf-rel "stage_0_outputs/meddra.csv")
        mesh-des-subClassOf (get-subClassOf-rel "stage_0_outputs/mesh_des.csv")
        mesh-scr-subClassOf (get-subClassOf-rel "stage_0_outputs/mesh_scr.csv")
        mondo-subClassOf (get-subClassOf-rel "stage_0_outputs/mondo.csv")
        ncit-subClassOf (get-subClassOf-rel "stage_0_outputs/ncit.csv")
        orphanet-subClassOf (get-subClassOf-rel "stage_0_outputs/orphanet.csv")
        snomed-subClassOf (get-subClassOf-rel "stage_0_outputs/snomedct.csv")
        umls-subClassOf (get-subClassOf-rel "stage_0_outputs/umls.csv")
        medgen-subClassOf (get-subClassOf-rel "stage_0_outputs/medgen.csv")
        subClassOf-combined (->> (concat doid-subClassOf efo-subClassOf hpo-subClassOf
                                         mesh-des-subClassOf mesh-scr-subClassOf
                                         icd10-subClassOf icdo-subClassOf
                                         icd11-subClassOf mondo-subClassOf orphanet-subClassOf kegg-subClassOf
                                         icd9-subClassOf meddra-subClassOf ncit-subClassOf snomed-subClassOf
                                         umls-subClassOf medgen-subClassOf)
                                 (filter #(not= (:start %) (:end %))))
        disease-nodes (disease-nodes "stage_1_outputs/disease_nodes.csv")
        subClassOf-rel (-> (kg/joiner subClassOf-combined (map #(set/rename-keys % {:id :end_id}) disease-nodes) :end :source_id kg/inner-join)
                           (kg/joiner (map #(set/rename-keys % {:id :start_id}) disease-nodes) :start :source_id kg/inner-join))]
    (->> (map #(assoc % :type "subClassOf") subClassOf-rel)
         (mapv #(select-keys % [:start_id :type :end_id]))
         distinct
         (kg/write-csv [:start_id :type :end_id] "./resources/stage_2_outputs/subClassOf_rel.csv"))))