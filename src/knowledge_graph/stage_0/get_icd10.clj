(ns knowledge-graph.stage-0.get-icd10
  (:require
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip :as d-zip]
   [clojure.data.zip.xml :refer [text xml->]]
   [clojure.zip :as z]
   [clojure.string :as str]
   [clojure.set :as set]
   [knowledge-graph.module.module :as kg]))

(defn descent=
  "Returns a query predicate that matches a node when its is a tag
  named tagname."
  [tagname]
  (fn [loc] (filter #(and (z/branch? %) (= tagname (:tag (z/node %))))
            (d-zip/children-auto loc))))

(defn flatten-column
  [m]
  (let [{:keys [id label synonym]} m]
    (map (fn [x] {:id id :label label :synonym x}) synonym)))

(defn three-diag-depth-children
  "Extract information from xml elements and create a map"
  [data]
  (let [z (z/xml-zip data)
        diags (xml-> z :chapter :section :diag (descent= :diag) (descent= :diag))]
    (for [diag diags :let [ synonym (xml-> diag :inclusionTerm :note)
                            id (xml-> diag :name)
                            label (xml-> diag :desc)]]
        {:id (str/join "" (map text id)) :label (str/join "" (map text label)) :synonym (str/join "|" (map text synonym))})))

(defn two-diag-depth-children
  "Extract information from xml elements and create a map"
  [data]
  (let [z (z/xml-zip data)
        diags (xml-> z :chapter :section :diag (descent= :diag))]
    (for [diag diags :let [ synonym (xml-> diag :inclusionTerm :note)
                            id (xml-> diag :name)
                            label (xml-> diag :desc)]]
        {:id (str/join "" (map text id)) :label (str/join "" (map text label)) :synonym (str/join "|" (map text synonym))})))

(defn one-diag-depth-children
  "Extract information from xml elements and create a map"
  [data]
  (let [z (z/xml-zip data)
        diags (xml-> z :chapter :section :diag)]
    (for [diag diags :let [ synonym (xml-> diag :includes :note)
                            id (xml-> diag :name)
                            label (xml-> diag :desc)]]
        {:id (str/join "" (map text id)) :label (str/join "" (map text label)) :synonym (str/join "|" (map text synonym))})))
      
(defn process-diag-data
  [diag-data]
  (->> (apply concat diag-data)
       (map #(assoc % :synonym (str/split (:synonym %), #"\|")))
       (map #(assoc % :data (flatten-column %)))     
       (map #(:data %))
       (apply concat)
       (filter #(not (str/blank? (:synonym %))))))

(defn get-results
  "Stream xml file, parse for necessary information, and write as csv output"
  [url]
  (let [data (->> (client/get url {:as :stream})
                  :body
                  (d-xml/parse)
                  :content
                  (filter #(= (:tag %) :chapter)))
        three-diag-children-data (->> (map three-diag-depth-children data)
                                      (process-diag-data))
        two-diag-children-data (->> (map two-diag-depth-children data)
                                    (process-diag-data))
        one-diag-depth-children (->> (map one-diag-depth-children data)
                                     (process-diag-data))
        icd10-data (->> (concat one-diag-depth-children two-diag-children-data three-diag-children-data)
                        (map #(assoc % :subClassOf (first (str/split (:id %) #"\."))))
                        (map #(assoc % :source_id (str/replace (:id %), #"\." "")))
                        (map #(assoc % :id (str/join "_" ["ICD10CM" (str/replace (:id %), #"\." "")]))))]
      (mapv #(select-keys % [:id :label :synonym :source_id :subClassOf]) icd10-data)))

(defn get-icd10-mapping
  [core-file-path icd10-file-path]
  (with-open [core-file (io/reader (io/resource core-file-path))
              icd10-file (io/reader (io/resource icd10-file-path))]
    (let [data-core (->> (csv/read-csv core-file :separator \tab)
                          kg/csv->map
                          (map #(select-keys % [:SCTID :Fully_Specified_Name :ICD_10_CM :Clinician_Friendly_Name :Patient_Friendly_Name])))
          data-icd10 (->> (csv/read-csv icd10-file :separator \tab)
                          kg/csv->map
                          (map #(select-keys % [:referencedComponentId :referencedComponentName :mapTarget :mapTargetName])))
          synonym_1 (->> (map #(set/rename-keys % {:SCTID :hasDbXref :ICD_10_CM :source_id :Fully_Specified_Name :synonym_1}) data-core))
          synonym_2 (->> (map #(set/rename-keys % {:SCTID :hasDbXref :ICD_10_CM :source_id :Clinician_Friendly_Name :synonym_1}) data-core))
          synonym_3 (->> (map #(set/rename-keys % {:SCTID :hasDbXref :ICD_10_CM :source_id :Patient_Friendly_Name :synonym_1}) data-core))
          synonym_4 (->> (map #(set/rename-keys % {:referencedComponentId :hasDbXref :mapTarget :source_id :mapTargetName :synonym_1}) data-icd10))
          synonym_5 (->> (map #(set/rename-keys % {:referencedComponentId :hasDbXref :mapTarget :source_id :referencedComponentName :synonym_1}) data-icd10))]
      (->> (concat synonym_1 synonym_2 synonym_3 synonym_4 synonym_5)
           (map #(assoc % :dbXref_source "SNOMEDCT"))
           (map #(assoc % :hasDbXref (str/replace (:hasDbXref %) "." "")))
           (filter #(some? (:source_id %)))
           (mapv #(select-keys % [:source_id :hasDbXref :dbXref_source :synonym_1]))))))
      
(def output-path "./resources/stage_0_outputs/icd10.csv")
(def info-url "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2022/icd10cm_tabular_2022.xml")
(def snomed->icd10-file "downloads/SNOMED_CT_to_ICD-10-CM_Resources_20220901/tls_Icd10cmHumanReadableMap_US1000124_20220901.tsv")
(def snomed-core-file "downloads/UMLS_KP_ProblemList_Mapping_20220301_Final.txt")

(defn run [_]
  (let [icd10-info (get-results info-url)
        icd10-mapping (get-icd10-mapping snomed-core-file snomed->icd10-file)
        icd10-combined (kg/joiner icd10-info icd10-mapping :source_id :source_id kg/left-join)
        icd10-synonym (map #(select-keys % [:id :label :source_id :synonym :subClassOf :hasDbXref :dbXref_source]) icd10-combined) 
        icd10-synonym-1 (->> (map #(set/rename-keys % {:synonym_1 :synonym :synonym :_}) icd10-combined)
                             (map #(select-keys % [:id :label :source_id :synonym :subClassOf :hasDbXref :dbXref_source])))
        icd10 (concat icd10-synonym icd10-synonym-1)]
    (->> (filter #(not= (:label %) (:synonym %)) icd10)
         distinct
         (kg/write-csv [:id :label :source_id :synonym :subClassOf :hasDbXref :dbXref_source] output-path))))