(ns knowledge-graph.stage-0.get-ncit
  (:require
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

(defn get-ncit
  [url]
  (let [ncit-raw (->> (client/get url {:as :reader})
                  :body
                  csv/read-csv 
                  csv->map
                  (map #(assoc % :id (:code %)))
                  (map #(assoc % :label (:preferredTerm %)))
                  (map #(assoc % :synonym (:synonyms %)))
                  (map #(assoc % :synonym (str/split (:synonym %) #"\|")))
                  (map #(select-keys % [:id :label :synonym])))
        ncit (->> (map #(flatten-synonym %) ncit-raw)
                  (apply concat))]
        (map #(select-keys % [:id :label :synonym]) ncit)))

(defn get-ncit-mapping
  [neoplasm-url meddra-url]
  (let [data-neoplasm-map (->> (client/get neoplasm-url {:as :reader})
                                :body
                                csv/read-csv
                                csv->map)
        data-meddra-map (->> (client/get meddra-url {:as :reader})
                              :body
                              csv/read-csv
                              csv->map)
        ncit-umls-mapping (->> (map #(set/rename-keys % {:ncItCode :id :ncImCui :hasDbXref :ncImPreferredName :synonym}) data-neoplasm-map)
                                (map #(assoc % :dbXref_source "UMLS"))
                                (mapv #(select-keys % [:id :hasDbXref :dbXref_source :synonym])))
        ncit-medgen-mapping (->> (map #(set/rename-keys % {:ncItCode :id :ncImCui :hasDbXref :ncImPreferredName :synonym}) data-neoplasm-map)
                                  (map #(assoc % :dbXref_source "MEDGEN"))
                                  (mapv #(select-keys % [:id :hasDbXref :dbXref_source :synonym])))
        ncit-neoplasm-mapping (->> (map #(set/rename-keys % {:ncItCode :id :sourceCode :hasDbXref :ncImSource :dbXref_source :sourceTerm :synonym}) data-neoplasm-map)
                                    (map #(assoc % :hasDbXref (kg/correct-source-id (:hasDbXref %))))
                                    (map #(assoc % :dbXref_source (kg/create-source (:dbXref_source %) (:dbXref_source %))))
                                    (mapv #(select-keys % [:id :hasDbXref :dbXref_source :synonym])))
        ncit-meddra-mapping (->> (map #(assoc % :dbXref_source "MedDRA") data-meddra-map) 
                                  (map #(set/rename-keys % {:CODE :id :TARGET_CODE :hasDbXref :TARGET_TERM :synonym}))
                                  (mapv #(select-keys % [:id :hasDbXref :dbXref_source :synonym])))
        ncit-mapping (->> (concat ncit-umls-mapping ncit-medgen-mapping ncit-neoplasm-mapping ncit-meddra-mapping)
                          distinct)]
        (->> (filter #(not= (:hasDbXref %) "") ncit-mapping)
             (map #(select-keys % [:id :hasDbXref :dbXref_source :synonym])))))

(def output-path "./resources/stage_0_outputs/ncit.csv")
(def info-url "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Neoplasm/Neoplasm_Core.csv")
(def neoplasm-url "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Neoplasm/Neoplasm_Core_Mappings_NCIm_Terms.csv")
(def meddra-url "https://ncit.nci.nih.gov/ncitbrowser/ajax?action=export_maps_to_mapping&target=MedDRA")

(defn run [_]
  (let [ncit-info (get-ncit info-url)
        ncit-mapping (get-ncit-mapping neoplasm-url meddra-url)]
    (->> (kg/joiner ncit-info ncit-mapping :id :id kg/left-join)
         (map #(assoc % :source_id (:id %)))
         (map #(assoc % :id (str/join "_" ["NCIT" (:id %)])))
         (kg/write-csv [:id :label :source_id :synonym :hasDbXref :dbXref_source] output-path))))

