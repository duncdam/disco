(ns knowledge-graph.stage-0.get-ncit
  (:require
   [clojure.java.io :as io]
   [camel-snake-kebab.core :as csk]
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [clojure.set :as set]
   [knowledge-graph.module.module :as kg]))

(defn csv->map
  [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map #(csk/->camelCase %))
            (map keyword)
            repeat)
       (rest csv-data)))

(defn flatten-synonym
  [m]
  (let [{:keys [id label synonym]} m]
    (map (fn [x] {:id id :label label :synonym x}) synonym)))

(defn flatten-subClassOf
  [m]
  (let [{:keys [id subClassOf]} m]
    (map (fn [x] {:id id :subClassOf x}) subClassOf)))

(defn get-ncit
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data (->> (kg/lines-reducible file)
                    vec
                    (map #(str/split % #"\t"))
                    (cons ["id" "_" "subClassOf" "synonym" "_" "label" "status" "type"])
                    kg/csv->map
                    (filter #(not (str/blank? (:type %))))
                    ;; filter for disease and syndrome semantic types only
                    (filter #(or
                              (str/includes? (:type %) "Finding")
                              (str/includes? (:type %) "Disease or Syndrome")
                              (str/includes? (:type %) "Mental or Behavioral Dysfunction")
                              (str/includes? (:type %) "Neoplastic Process")
                              (str/includes? (:type %) "Injury or Poisoning")
                              (str/includes? (:type %) "Pathologic Function")
                              (str/includes? (:type %) "Sign or Symptom")))
                    (filter #(not (or
                                   (str/includes? (:synonym %) "Mouse")
                                   (str/includes? (:synonym %) "Rat"))))
                    (filter #(not (str/includes? (str/lower-case (:status %)) "obsolete")))
                    (map #(select-keys % [:id :synonym :subClassOf :label]))
                    (map #(assoc % :subClassOf (str/split (:subClassOf %) #"\|")))
                    (map #(assoc % :synonym (str/split (:synonym %) #"\|")))
                    (map #(assoc % :label (cond
                                            (not (str/blank? (:label %))) (:label %)
                                            :else (first (:synonym %))))))
          unpacked-synonym (->> (map #(select-keys % [:id :label :synonym]) data)
                                (map #(assoc % :unpacked_synonym (flatten-synonym %)))
                                (map #(:unpacked_synonym %))
                                (apply concat)
                                (filter #(not= (:label %) (:synonym %)))
                                (filter #(not (str/blank? (:id %)))))
          unpacked-subClassOf (->> (map #(select-keys % [:id :subClassOf]) data)
                                   (map #(assoc % :unpacked_subClassOf (flatten-subClassOf %)))
                                   (map #(:unpacked_subClassOf %))
                                   (apply concat))]
      (kg/joiner unpacked-synonym unpacked-subClassOf :id :id kg/left-join))))

(defn get-ncit-mapping
  [neoplasm-url umls-url]
  (let [data-neoplasm-map (->> (client/get neoplasm-url {:as :reader})
                               :body
                               csv/read-csv
                               csv->map)
        data-umls-map (->> (csv/read-csv (->> (client/get umls-url {:as :reader})
                                              :body)
                                         :separator \|)
                           (cons ["id" "hasDbXref"])
                           (map #(take 2 %))
                           csv->map)
        ncit-umls-mapping (->> (map #(assoc % :dbXref_source "UMLS") data-umls-map)
                               (filter #(not (str/includes? (:hasDbXref %) "CL")))
                               (mapv #(select-keys % [:id :hasDbXref :dbXref_source])))
        ncit-medgen-mapping (->> (map #(assoc % :dbXref_source "MEDGEN") data-umls-map)
                                 (filter #(not (str/includes? (:hasDbXref %) "CL")))
                                 (mapv #(select-keys % [:id :hasDbXref :dbXref_source])))
        ncit-neoplasm-mapping (->> (map #(set/rename-keys % {:ncItCode :id :sourceCode :hasDbXref :ncImSource :dbXref_source}) data-neoplasm-map)
                                   (map #(assoc % :hasDbXref (kg/correct-xref-id (:hasDbXref %))))
                                   (map #(assoc % :dbXref_source (kg/correct-source (:dbXref_source %))))
                                   (mapv #(select-keys % [:id :hasDbXref :dbXref_source])))
        ncit-mapping (->> (concat ncit-umls-mapping ncit-medgen-mapping ncit-neoplasm-mapping)
                          distinct)]
    (->> (filter #(not= (:hasDbXref %) "") ncit-mapping)
         (map #(select-keys % [:id :hasDbXref :dbXref_source :synonym])))))

(def output-path "./resources/stage_0_outputs/ncit.csv")
(def ncit-file-path "downloads/Thesaurus.txt")
(def neoplasm-url "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Neoplasm/Neoplasm_Core_Mappings_NCIm_Terms.csv")
(def umls-url "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/nci_code_cui_map_202208.dat")

(defn run [_]
  (let [ncit-info (get-ncit ncit-file-path)
        ncit-mapping (get-ncit-mapping neoplasm-url umls-url)]
    (->> (kg/joiner ncit-info ncit-mapping :id :id kg/left-join)
         (map #(assoc % :source_id (:id %)))
         (map #(select-keys % [:id :label :source_id :subClassOf :synonym :hasDbXref :dbXref_source]))
         distinct
         (kg/write-csv [:id :label :source_id :subClassOf :synonym :hasDbXref :dbXref_source] output-path))))

