(ns knowledge-graph.stage-0.get-mondo
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [attr text xml->]]
   [clojure.string :as str]
   [clojure.zip :as z]
   [knowledge-graph.module.module :as kg]))

(defn class-map
  "Extract information from xml elements and create a map"
  [data]
  (let [z (z/xml-zip data)]
    (for [id         (xml-> z :Class (attr :rdf/about))
          label      (xml-> z :Class :label text)
          source_id  (xml-> z :Class :id text)
          subClassOf (xml-> z :Class :subClassOf (attr :rdf/resource))
          dbXref     (xml-> z :Class :hasDbXref text)
          synonym        (concat (xml-> z :Class :hasExactSynonym text) (xml-> z :Class :hasRelatedSynonym text))
          is_disease     (xml-> z :Class :inSubset (attr :rdf/resource))]
      {:id id :label label :source_id source_id :subClassOf subClassOf :hasDbXref dbXref :synonym synonym :is_disease is_disease})))

(defn get-results
  "Download xml file, parse for necessary information, and write as csv output"
  [url output_path]
  (->> (client/get url {:as :stream})
       :body
       (d-xml/parse)
       :content
       (filter #(= (:tag %) :Class))
       (map class-map)
       (apply concat)
       (filter #(some? (:id %)))
       (filter #(or (str/includes? (:is_disease %) "disease") (str/includes? (:is_disease %) "disorder") (str/includes? (:is_disease %) "syndrome")))
       (map #(assoc % :id (last (str/split (:id %) #"/"))))
       (map #(assoc % :subClassOf (last (str/split (:subClassOf %) #"/"))))
       (map #(assoc % :dbXref_source (first (str/split (:hasDbXref %) #":"))))
       (map #(assoc % :dbXref_source (kg/correct-source (:dbXref_source %))))
       (map #(assoc % :hasDbXref (kg/correct-xref-id (:hasDbXref %))))
       (map #(assoc % :hasDbXref (str/replace (:hasDbXref %) "." "")))
       (map #(select-keys % [:id :label :subClassOf :hasDbXref :synonym :dbXref_source]))
       distinct
       (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output_path)))

(defn run [_]
  (let [url "https://www.ebi.ac.uk/ols/ontologies/mondo/download"
        output "./resources/stage_0_outputs/mondo.csv"]
    (get-results url output)))

