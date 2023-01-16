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
                    (map #(assoc % :dbXref_source "SNOMEDCT"))
                    (map #(assoc % :id (:meddra_id %)))
                    (map #(assoc % :hasDbXref (:snomed_id %)))
                    (map #(assoc % :source_id  (:meddra_id %)))
                    (map #(assoc % :synonym (:label %)))
                    (map #(assoc % :label (->> (remove #{"(disorder)" "(finding)" "(procedure)" "(situation)"} (str/split (:label %) #" "))
                                               (str/join " "))))
                    (mapv #(select-keys % [:id :label :source_id :hasDbXref :dbXref_source :synonym])))]
    meddra))

(defn get-meddra-from-others
  [file-path source]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (filter #(= (:dbXref_source %) "MEDDRA"))
         (map #(assoc % :new_source_id (:hasDbXref %)))
         (map #(assoc % :hasDbXref (:source_id %)))
         (map #(assoc % :id (:new_source_id %)))
         (map #(assoc % :source_id (:new_source_id %)))
         (map #(assoc % :dbXref_source source))
         (mapv #(select-keys % [:id :label :source_id :hasDbXref :dbXref_source :synonym])))))

(defn create-meddra
  [meddra-ncit-url meddra-snomed-path snomed-core-path
   stage-0-doid-path stage-0-efo-path
   stage-0-hpo-path stage-0-mondo-path
   stage-0-orphanet-path stage-0-umls-path]
  (let [meddra-ncit (get-meddra-ncit meddra-ncit-url)
        meddra-snomed (get-meddra-snomed meddra-snomed-path snomed-core-path)
        meddra-doid (get-meddra-from-others stage-0-doid-path "DOID")
        meddra-efo (get-meddra-from-others stage-0-efo-path "EFO")
        meddra-hpo (get-meddra-from-others stage-0-hpo-path "HPO")
        meddra-mondo (get-meddra-from-others stage-0-mondo-path "MONDO")
        meddra-orphanet (get-meddra-from-others stage-0-orphanet-path "ORPHANET")
        meddra-umls (get-meddra-from-others stage-0-umls-path "UMLS")
        meddra-combined (concat meddra-ncit meddra-snomed
                                meddra-doid meddra-efo
                                meddra-hpo meddra-mondo
                                meddra-orphanet meddra-umls)
        meddra-name-syn (->> meddra-combined
                             (group-by :id)
                             (map #(into {} {:id (first %)
                                             :label (first (sort (distinct (map (fn [x] (:label x)) (second %)))))
                                             :synonym (second (sort (distinct (map (fn [x] (:label x)) (second %)))))})))
        meddra-name (map #(select-keys % [:id :label]) meddra-name-syn)
        meddra-syn (concat (map #(select-keys % [:id :synonym]) meddra-combined)
                           (->> (map #(select-keys % [:id :synonym]) meddra-name-syn)
                                (filter #(not (str/blank? (:synonym %))))))
        meddra (-> (->> meddra-combined
                        (map #(select-keys % [:id :source_id :hasDbXref :dbXref_source])))
                   (kg/joiner meddra-name :id :id kg/inner-join)
                   (kg/joiner meddra-syn :id :id kg/left-join))]
    meddra))

(def meddra-ncit-url "https://ncit.nci.nih.gov/ncitbrowser/ajax?action=export_maps_to_mapping&target=MedDRA")
(def meddra-snomed-path "downloads/SnomedCT_SNOMEDMedDRAMapPackage_PRODUCTION_20220511T120000Z/Full/Refset/Map/der2_sRefset_SNOMEDtoMedDRASimpleMapFull_INT_20220131.txt")
(def snomed-core-path "downloads/SNOMEDCT_CORE_SUBSET_202211.txt")
(def stage-0-doid-path "stage_0_outputs/doid.csv")
(def stage-0-efo-path "stage_0_outputs/efo.csv")
(def stage-0-hpo-path "stage_0_outputs/hpo.csv")
(def stage-0-mondo-path "stage_0_outputs/mondo.csv")
(def stage-0-orphanet-path "stage_0_outputs/orphanet.csv")
(def stage-0-umls-path "stage_0_outputs/umls.csv")
(def output-path "./resources/stage_0_outputs/meddra.csv")

(defn run
  [_]
  (->> (create-meddra meddra-ncit-url meddra-snomed-path snomed-core-path
                      stage-0-doid-path stage-0-efo-path
                      stage-0-hpo-path stage-0-mondo-path
                      stage-0-orphanet-path stage-0-umls-path)
       (map #(assoc % :subClassOf ""))
       distinct
       (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path)))