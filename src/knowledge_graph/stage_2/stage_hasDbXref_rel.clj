(ns knowledge-graph.stage-2.stage-hasDbXref-rel
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))


(defn hasDbXref
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         (kg/csv->map)
         (map #(assoc % :dbXref (kg/correct-source-id (:hasDbXref %))))
         (map #(assoc % :end (:dbXref %)))
         (filter #(not (str/blank? (:end %))))
         (map #(assoc % :start (last (str/split (:id %) #"/"))))
         (mapv #(select-keys % [:start :end :dbXref_source]))
         distinct)))

(defn disease-nodes
  [disease-file-path]
  (with-open [d-file (io/reader (io/resource disease-file-path))]
    (->> (csv/read-csv d-file :separator \tab)
         (kg/csv->map)
         (mapv #(select-keys % [:id :source_id :source]))
         distinct)))

(defn run [_]
  (let [doid-dbXref (hasDbXref "stage_0_outputs/doid.csv")
        efo-dbXref (hasDbXref "stage_0_outputs/efo.csv")
        hpo-dbXref (hasDbXref "stage_0_outputs/hpo.csv")
        icd9-dbXref (hasDbXref "stage_0_outputs/icd9_mapping.csv")
        medgen-dbXref (hasDbXref "stage_0_outputs/medgen_mapping.csv")
        mondo-dbXref (hasDbXref "stage_0_outputs/mondo.csv")
        ncit-dbXref (hasDbXref "stage_0_outputs/ncit_mapping.csv")
        orphanet-dbXref (hasDbXref "stage_0_outputs/orphanet.csv")
        snomedct-dbXref (hasDbXref "stage_0_outputs/snomedct_mapping.csv")
        snomedct-core-dbXref (hasDbXref "stage_0_outputs/snomedct.csv")
        umls-dbXref (hasDbXref "stage_0_outputs/umls.csv")
        kegg-dbXref (hasDbXref "stage_0_outputs/kegg_mapping.csv")
        dbXref (distinct (concat doid-dbXref efo-dbXref 
                                 hpo-dbXref mondo-dbXref 
                                 orphanet-dbXref 
                                 umls-dbXref
                                 kegg-dbXref
                                 icd9-dbXref medgen-dbXref 
                                 ncit-dbXref 
                                 snomedct-dbXref snomedct-core-dbXref))
        disease (disease-nodes "stage_1_outputs/disease_nodes.csv")
        disease-dbXref (-> (map #(set/rename-keys % {:id :end_id}) disease)
                           (kg/joiner dbXref :source_id :end  kg/inner-join)
                           (kg/joiner disease :start :source_id kg/inner-join))
        cleaned-dbXref (->> (filter #(= (:source %) (:dbXref_source %)) disease-dbXref)
                            (map #(set/rename-keys % {:id :start_id}))
                            (map #(select-keys % [:start_id :end_id])))
        reversed-cleaned-dbXref(->> (map #(set/rename-keys % {:start_id :end_id :end_id :start_id}) cleaned-dbXref))
        ]
        (->> 
             (concat cleaned-dbXref reversed-cleaned-dbXref)
             (filter #(not= (:start_id %) (:end_id %)))
             (map #(assoc % :type "hasDbXref"))
             distinct
             (kg/write-csv [:start_id :type :end_id] "./resources/stage_2_outputs/hasDbXref_rel.csv")
             )))