(ns knowledge-graph.stage-0.parse-hpo
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [attr text xml-> ]]
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
          {:id id :label label :alternative_id alternative_id :subClassOf subClassOf :dbXref dbXref :synonym synonym})))

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
   distinct
   (kg/write-csv [:id :label :alternative_id :subClassOf :dbXref :synonym] output_path)
  ))

(defn run []
  (let [url "http://purl.obolibrary.org/obo/hp.owl"
        output "./resources/stage_0_outputs/hpo.csv"]
    (get-results url output)))

