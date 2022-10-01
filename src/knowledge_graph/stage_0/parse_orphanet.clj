(ns knowledge-graph.stage-0.parse-orphanet
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [text attr xml->]]
   [clojure.zip :as z]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn orphanet-map
  [data]
  (let [z (z/xml-zip data)]
    (for [id (xml-> z :Class (attr :rdf/about))
          name (xml-> z :Class :label text)      
          subClassOf (xml-> z :Class :subClassOf (attr :rdf/resource))
          dbXref (xml-> z :Class :hasDbXref text)
          synonym (xml-> z :Class :alternative_term text)
          source_id (xml-> z :Class :notation text)]
      {:id id :name name :subClassOf subClassOf :dbXref dbXref :synonym synonym :source_id source_id})))

(defn get-orphanet
  [url output-path]
  (->>
   (client/get url {:as :stream})
   :body
   d-xml/parse
   :content
   (filter #(= (:tag %) :Class))
   (map orphanet-map)
   (apply concat)
   (filter #(not= (str/lower-case (:source_id %)) "clinical subtype"))
   distinct
   (kg/write-csv [:id :name :subClassOf :dbXref :synonym :source_id] output-path)))

(defn run[]
  (let [url "https://www.orphadata.com/data/ontologies/ordo/last_version/ORDO_en_4.1.owl"
        output-path-orphanet "./resources/stage_0_outputs/orphanet.csv"]
    (get-orphanet url output-path-orphanet)))