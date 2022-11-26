(ns knowledge-graph.stage-0.get-orphanet
  (:require
   [clojure.java.io :as io]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [text xml->]]
   [clojure.zip :as z]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn orphanet-map
  [data]
  (let [z (z/xml-zip data)
        disorders (xml-> z :DisorderList :Disorder)]
    (for [disorder disorders 
          :let [id (xml-> disorder :OrphaCode)
                label (xml-> disorder :Name)
                dbXref (xml-> disorder :ExternalReferenceList :ExternalReference :Reference)
                dbXref_source (xml-> disorder :ExternalReferenceList :ExternalReference :Source)
                dbXref_mapping (xml-> disorder :ExternalReferenceList :ExternalReference :DisorderMappingRelation :Name)
                synonym (xml-> disorder :SynonymList :Synonym)]]
      {:id (str/join "" (map text id))
       :label (str/join "" (map text label))
       :hasDbXref (->> (interleave (map text dbXref_source) (map text dbXref) (map text dbXref_mapping))
                       (partition 3)
                       (map #(str/join "_" [(first %) (second %) (last %)])))
       :synonym (map text synonym)})))
       

(defn orphanet-subclass-map
  [data]
  (let [z (z/xml-zip data)
        disorders (xml-> z :DisorderList :Disorder)]
    (for [disorder disorders :let [id (xml-> disorder :OrphaCode)
                                   subClassOf (xml-> disorder :DisorderDisorderAssociationList :DisorderDisorderAssociation :TargetDisorder :OrphaCode)]]
      {:id (str/join "" (map text id))
       :subClassOf (str/join "" (map text subClassOf))})))

(defn flatten-dbXref
  [m]
  (let [{:keys [id label hasDbXref synonym]} m]
    (map (fn [x] {:id id :label label :hasDbXref x :synonym synonym}) hasDbXref)))

(defn flatten-synonym
  [m]
  (let [{:keys [id label synonym hasDbXref]} m]
    (map (fn [x] {:id id :label label :synonym x :hasDbXref hasDbXref}) synonym)))

(defn get-orphanet-subClassOf
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (d-xml/parse file)
         :content
         (filter #(= (:tag %) :DisorderList))
         (map orphanet-subclass-map)
         (apply concat)
         (map #(assoc % :id (str/join "_" ["ORPHANET" (:id %)])))
         (map #(assoc % :subClassOf (str/join ":" ["ORPHA" (:subClassOf %)])))
         (map #(select-keys % [:id :subClassOf]))
         distinct)))

(defn get-orphanet
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (d-xml/parse file)
         :content
         (filter #(= (:tag %) :DisorderList))
         (map orphanet-map)
         (apply concat)
         (map #(assoc % :hasDbXref (flatten-dbXref %)))
         (map #(:hasDbXref %))
         (apply concat)
         (map #(assoc % :synonym (flatten-synonym %)))
         (map #(:synonym %))
         (apply concat)
         (filter #(str/includes? (:hasDbXref %) "Exact mapping"))
         (map #(assoc % :hasDbXref (str/split (:hasDbXref %) #"_")))
         (map #(assoc % :hasDbXref (str/join ":" [(first (:hasDbXref %)) (second (:hasDbXref %))])))
         (map #(assoc % :dbXref_source (first (str/split (:hasDbXref %) #":"))))
         (map #(assoc % :dbXref_source (kg/correct-source (:dbXref_source %))))
         (map #(assoc % :hasDbXref (last (str/split (:hasDbXref %) #":"))))
         (map #(assoc % :hasDbXref (kg/correct-xref-id (:hasDbXref %))))
         (map #(assoc % :source_id (str/join ":" ["ORPHA" (:id %)])))
         (map #(assoc % :id (str/join "_" ["ORPHANET" (:id %)])))
         (map #(select-keys % [:id :label :source_id :hasDbXref :dbXref_source :synonym]))
         distinct)))

(def file-path-info "downloads/en_product1.xml")
(def file-path-subClassOf "downloads/en_product7.xml")
(def output-path "./resources/stage_0_outputs/orphanet.csv")

(defn run [_]
  (let [orphanet-info (get-orphanet file-path-info)
        orphanet-subClassOf (get-orphanet-subClassOf file-path-subClassOf)]
    (->> (kg/joiner orphanet-info orphanet-subClassOf :id :id kg/left-join)   
         (map #(select-keys % [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym]))
         (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path))))