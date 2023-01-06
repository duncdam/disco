(ns knowledge-graph.stage-0.get-meddra
  (:require
   [clj-http.client :as client]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-meddra-ncit
  [url]
  (let [meddra-ncit (->> (client/get url {:as :reader})
                         :body
                         csv/read-csv
                         kg/csv->map
                         (map #(set/rename-keys % {:TARGET_CODE :id :TARGET_TERM :label :PT :synonym :CODE :hasDbXref}))
                         (map #(assoc % :dbXref_source "NCIT"))
                         (map #(assoc % :source_id (:id %))))]
    meddra-ncit))

(defn get-meddra-snomed
  [meddra-snomed-path snomed-core-path]
  (let [meddra-snomed (with-open [file (io/reader (io/resource meddra-snomed-path))]
                        (->> (csv/read-csv file :separator \tab)
                             kg/csv->map
                             (map #(set/rename-keys % {:referencedComponentId :snomed_id :mapTarget :meddra_id}))
                             (mapv #(select-keys % [:snomed_id :meddra_id]))))
        snomed-core (with-open [file (io/reader (io/resource snomed-core-path))]
                      (->> (csv/read-csv file :separator \|)
                           kg/csv->map
                           (filter #(= (:SNOMED_CONCEPT_STATUS %) "Current"))
                           (mapv #(set/rename-keys % {:SNOMED_CID :snomed_id :SNOMED_FSN :label}))))
        meddra (->> (kg/joiner meddra-snomed snomed-core :snomed_id :snomed_id kg/inner-join)
                    (map #(assoc % :dbXref_source "SNOMED"))
                    (map #(assoc % :id (:meddra_id %)))
                    (map #(assoc % :hasDbXref (:snomed_id %)))
                    (map #(assoc % :source_id  (:meddra_id %)))
                    (map #(assoc % :synonym (:label %)))
                    (map #(assoc % :label (->> (remove #{"(disorder)" "(finding)" "(procedure)" "(situation)"} (str/split (:label %) #" "))
                                               (str/join " "))))
                    (mapv #(select-keys % [:id :label :source_id :hasDbXref :dbXref_source :synonym])))]
    meddra))

(defn create-meddra
  [meddra-ncit-url meddra-snomed-path snomed-core-path]
  (let [meddra-ncit (get-meddra-ncit meddra-ncit-url)
        meddra-snomed (get-meddra-snomed meddra-snomed-path snomed-core-path)
        meddra-name-syn (->> (concat meddra-ncit meddra-snomed)
                             (group-by :id)
                             (map #(into {} {:id (first %)
                                             :label (first (sort (distinct (map (fn [x] (:label x)) (second %)))))
                                             :synonym (second (sort (distinct (map (fn [x] (:label x)) (second %)))))})))
        meddra-name (map #(select-keys % [:id :label]) meddra-name-syn)
        meddra-syn (concat (map #(select-keys % [:id :synonym]) meddra-ncit)
                           (map #(select-keys % [:id :synonym]) meddra-snomed)
                           (->> (map #(select-keys % [:id :synonym]) meddra-name-syn)
                                (filter #(not (str/blank? (:synonym %))))))
        meddra (-> (->> (concat meddra-ncit meddra-snomed)
                        (map #(select-keys % [:id :source_id :hasDbXref :dbXref_source])))
                   (kg/joiner meddra-name :id :id kg/inner-join)
                   (kg/joiner meddra-syn :id :id kg/left-join))]
    meddra))

(def meddra-ncit-url "https://ncit.nci.nih.gov/ncitbrowser/ajax?action=export_maps_to_mapping&target=MedDRA")
(def meddra-snomed-path "downloads/SnomedCT_SNOMEDMedDRAMapPackage_PRODUCTION_20220511T120000Z/Full/Refset/Map/der2_sRefset_SNOMEDtoMedDRASimpleMapFull_INT_20220131.txt")
(def snomed-core-path "downloads/SNOMEDCT_CORE_SUBSET_202205.txt")
(def output-path "./resources/stage_0_outputs/meddra.csv")

(defn run
  [_]
  (->> (create-meddra meddra-ncit-url meddra-snomed-path snomed-core-path)
       (map #(assoc % :subClassOf ""))
       distinct
       (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path)))