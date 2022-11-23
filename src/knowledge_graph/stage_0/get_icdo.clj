(ns knowledge-graph.stage-0.get-icdo
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set :as set]
   [camel-snake-kebab.core :as csk]
   [knowledge-graph.module.module :as kg]))

(defn get-icdo
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (->> (csv/read-csv file :separator \tab)
                    kg/csv->map
                    (map #(assoc % :id (str/replace (:Code %) #"\/" ""))))
          icdo-info (->> (map #(assoc % :source_id (:Code %)) data)
                         (map #(assoc % :label (cond
                                                 (= (:Struct %) "title") (:Label %))))
                         (filter #(not (str/blank? (:label %))))
                         (map #(select-keys % [:id :label :source_id])))
          icdo-synonym (->> (map #(assoc % :synonym (cond
                                                      (= (:Struct %) "sub") (:Label %))) data)
                            (filter #(not (str/blank? (:synonym %))))
                            (map #(select-keys % [:id :synonym])))
          icdo-subClassOf (->> (map #(assoc % :id_base (first (str/split (:Code %) #"\/"))) data)
                               (map #(assoc % :id_level (second (str/split (:Code %) #"\/"))))
                               (map #(assoc % :subClassOf (str/join "" [(:id_base %) "0"])))
                               (filter #(not= (:id_level %) "0"))
                               (map #(select-keys % [:id :subClassOf]))
                               distinct)
          icdo-data (-> (kg/joiner icdo-info icdo-subClassOf :id :id kg/left-join)
                        (kg/joiner icdo-synonym :id :id kg/left-join))]
      (->> icdo-data
           distinct))))

(defn csv->map
  [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map #(csk/->snake_case %))
            (map #(str/replace % #"\(|\)" ""))
            (map keyword)
            repeat)
       (rest csv-data)))

(defn get-icdo-mapping
  [mapping-url]
  (let [file (->> (client/get mapping-url {:as :reader})
                  :body)
        mapping-data (->> (csv/read-csv file :separator \tab)
                          csv->map
                          (map #(set/rename-keys % {:icd_o_code :id})))
        icdo-umls-data (->> (map #(assoc % :hasDbXref (:nc_im_cui %)) mapping-data)
                            (map #(assoc % :dbXref_source "UMLS"))
                            (map #(select-keys % [:id :hasDbXref :dbXref_source])))
        icdo-medgen-data (->> (map #(assoc % :hasDbXref (:nc_im_cui %)) mapping-data)
                              (map #(assoc % :dbXref_source "MEDGEN"))
                              (map #(select-keys % [:id :hasDbXref :dbXref_source])))
        icdo-ncit-data (->> (map #(assoc % :hasDbXref (:nc_it_code_if_present %)) mapping-data)
                            (map #(assoc % :dbXref_source "NCIT"))
                            (map #(select-keys % [:id :hasDbXref :dbXref_source])))]
    (->> (concat icdo-ncit-data icdo-medgen-data icdo-umls-data)
         (filter #(not (str/blank? (:hasDbXref %))))
         distinct)))

(def icdo-file-path "downloads/Morphenglish.txt")
(def icdo-mapping-url "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Mappings/ICD-O-3_Mappings/ICD-O-3.1-NCIt_Morphology_Mapping.txt")
(def output-path "./resources/stage_0_outputs/icdo.csv")

(defn run
  [_]
  (let [icdo-info (get-icdo icdo-file-path)
        icdo-mapping (get-icdo-mapping icdo-mapping-url)]
    (->> (kg/joiner icdo-info icdo-mapping :source_id :id kg/left-join)
         (kg/write-csv [:id :label :source_id :synonym :subClassOf :hasDbXref :dbXref_source] output-path))))