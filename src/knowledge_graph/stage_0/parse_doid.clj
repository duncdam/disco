(ns knowledge-graph.stage-0.parse-doid
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [attr text xml->]]
   [clojure.zip :as z]
   [knowledge-graph.module.module :as kg]))

(defn class-map
  "Extract information from xml elements and create a map"
  [data]
  (let [z (z/xml-zip data)]
    (for [id             (xml-> z :Class (attr :rdf/about))
          alternative_id (xml-> z :hasAlternativeId text)
          label          (xml-> z :label text)
          subClassOf     (xml-> z :subClassOf (attr :rdf/resource))
          dbXref         (xml-> z :hasDbXref text)
          synonym        (xml-> z :hasExactSynonym text)]
          {:id id :alternative_id alternative_id :label label :subClassOf subClassOf :dbXref dbXref :synonym synonym})))

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
       (kg/write-csv [:id :alternative_id :label :subClassOf :dbXref :synonym] output_path)))

(defn run []
  (let [url "https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/main/src/ontology/doid.owl"
        output "./resources/stage_0_outputs/doid.csv"]
    (get-results url output)))

