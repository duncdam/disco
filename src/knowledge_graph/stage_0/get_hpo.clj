(ns knowledge-graph.stage-0.get-hpo
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
          alternative_id  (xml-> z :Class :hasAlternativeId text)
          subClassOf  (xml-> z :Class :subClassOf (attr :rdf/resource))
          dbXref      (xml-> z :Class :hasDbXref text)
          synonym     (concat (xml-> z :Class :hasExactSynonym text) (xml-> z :Class :hasRelatedSynonym text) )]
          {:id id :label label :alternative_id alternative_id :subClassOf subClassOf :hasDbXref dbXref :synonym synonym})))

(defn get-results
  "Download xml file, parse for necessary information, and write as csv output"
  [url output_path]
  (->>
   (client/get url {:as :stream})
   :body
   (d-xml/parse)
   :content
   (filter #(= (:tag %) :Class))  
   (map class-map)
   (apply concat)
   (filter #(some? (:id %)))
   (filter #(str/includes? (:id %) "HP_"))
   (map #(assoc % :id (last (str/split (:id %) #"/"))))
   (map #(assoc % :subClassOf (last (str/split (:subClassOf %) #"/"))))
   (map #(assoc % :dbXref_source (kg/create-source (:hasDbXref %) "HPO")))
   (map #(assoc % :hasDbXref (kg/correct-source-id (:hasDbXref %))))
   (map #(assoc % :hasDbXref (str/replace (:hasDbXref %) "." "")))
   (map #(select-keys % [:id :label :subClassOf :hasDbXref :synonym :dbXref_source]))
   distinct
   (kg/write-csv [:id :label :subClassOf :hasDbXref :dbXref_source :synonym] output_path)))

(defn run [_]
  (let [url "http://purl.obolibrary.org/obo/hp.owl"
        output "./resources/stage_0_outputs/hpo.csv"]
    (get-results url output)))

