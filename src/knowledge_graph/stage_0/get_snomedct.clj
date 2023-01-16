(ns knowledge-graph.stage-0.get-snomedct
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn get-us-snomedct
  [snomed-us-file-path]
  (with-open [file (io/reader (io/resource snomed-us-file-path))]
    (->> (csv/read-csv file :separator \|)
         kg/csv->map
         (filter #(= (:SNOMED_CONCEPT_STATUS %) "Current"))
         (map #(set/rename-keys % {:SNOMED_CID :id :SNOMED_FSN :synonym}))
         (mapv #(select-keys % [:id :synonym]))
         distinct)))

(defn get-int-snomedct
  [snomed-int-rel-path snomed-int-des-path]
  (with-open [rel-file (io/reader (io/resource snomed-int-rel-path))
              des-file (io/reader (io/resource snomed-int-des-path))]
    (let [relationship (->> (csv/read-csv rel-file :separator \tab)
                            kg/csv->map
                            (filter #(or (and (= (:typeId %) "116680003")
                                              (= (:destinationId %) "64572001"))
                                         (= (:typeId %) "363698007")))
                            (map #(select-keys % [:sourceId]))
                            (mapv #(set/rename-keys % {:sourceId :id}))
                            distinct)
          concept (->> des-file
                       kg/lines-reducible
                       vec
                       (map #(str/split % #"\t"))
                       kg/csv->map
                       (filter #(not (str/blank? (:term %))))
                       (map #(select-keys % [:conceptId :term]))
                       (mapv #(set/rename-keys % {:conceptId :id :term :synonym}))
                       distinct)]
      (kg/joiner concept relationship :id :id kg/inner-join))))

(defn flatten-column
  [m]
  (let [{:keys [id label synonym]} m]
    (map (fn [x] {:id id :label label :synonym x}) synonym)))

(defn get-snomedct-concept
  [snomed-us-file-path snomed-int-rel-path snomed-int-des-path]
  (let [snomed-us (get-us-snomedct snomed-us-file-path)
        snomed-int (get-int-snomedct snomed-int-rel-path snomed-int-des-path)
        snomed-concept (->> (concat snomed-us snomed-int)
                            (group-by :id)
                            (map #(into {} {:id (first %)
                                            :label (first (sort (map (fn [x] (:synonym x)) (second %))))
                                            :synonym (rest (sort (map (fn [x] (:synonym x)) (second %))))}))
                            (map #(flatten-column %))
                            (apply concat)
                            (map #(assoc % :source_id (:id %)))
                            (map #(assoc % :subClassOf ""))
                            (mapv #(select-keys % [:id :label :synonym :source_id :subClassOf])))]
    snomed-concept))

(defn get-snomedct-mapping
  [snomed-us-path snomed-orphanet-mapping-file-path]
  (with-open [orphanet-file (io/reader (io/resource snomed-orphanet-mapping-file-path))
              umls-file (io/reader (io/resource snomed-us-path))]
    (let [orphanet-mapping (->> (csv/read-csv orphanet-file :separator \tab)
                                kg/csv->map
                                (map #(select-keys % [:referencedComponentId :mapTarget]))
                                (map #(set/rename-keys % {:referencedComponentId :id :mapTarget :hasDbXref}))
                                (map #(assoc % :hasDbXref (str/join ":" ["ORPHA" (:hasDbXref %)])))
                                (map #(assoc % :dbXref_source "ORPHANET"))
                                (mapv #(select-keys % [:id :hasDbXref :dbXref_source])))
          umls-mapping (->> (csv/read-csv umls-file :separator \|)
                            kg/csv->map
                            (filter #(= (:SNOMED_CONCEPT_STATUS %) "Current"))
                            (map #(set/rename-keys % {:SNOMED_CID :id :UMLS_CUI :hasDbXref}))
                            (map #(assoc % :dbXref_source "UMLS"))
                            (mapv #(select-keys  % [:id :hasDbXref :dbXref_source])))
          mapping (concat umls-mapping orphanet-mapping)]
      mapping)))

(def snomed-us-file-path "downloads/SNOMEDCT_CORE_SUBSET_202211.txt")
(def snomed-int-rel-path "downloads/SnomedCT_InternationalRF2_PRODUCTION_20221231T120000Z/Full/Terminology/sct2_Relationship_Full_INT_20221231.txt")
(def snomed-int-des-path "downloads/SnomedCT_InternationalRF2_PRODUCTION_20221231T120000Z/Full/Terminology/sct2_Description_Full-en_INT_20221231.txt")
(def snomed-orphanet-mapping-file-path "downloads/SnomedCT_SNOMEDOrphanetMapPackage_PRODUCTION_20211031T120000Z/Full/Refset/Map/der2_sRefset_OrphanetSimpleMapFull_INT_20210731.txt")
(def output-path "./resources/stage_0_outputs/snomedct.csv")

(defn run [_]
  (let [concept (get-snomedct-concept snomed-us-file-path snomed-int-rel-path snomed-int-des-path)
        mapping (get-snomedct-mapping snomed-us-file-path snomed-orphanet-mapping-file-path)]
    (->> (kg/joiner concept mapping :id :id kg/left-join)
         (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path))))

