(ns knowledge-graph.stage-0.get-doid
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [attr text xml->]]
   [clojure.zip :as z]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn class-map
  "Extract information from xml elements and create a map"
  [data]
  (let [z (z/xml-zip data)]
    (for [id             (xml-> z :Class (attr :rdf/about))
          alternative_id (xml-> z :hasAlternativeId text)
          label          (xml-> z :label text)
          subClassOf     (xml-> z :subClassOf (attr :rdf/resource))
          hasDbXref      (xml-> z :hasDbXref text)
          synonym        (xml-> z :hasExactSynonym text)]
          {:id id :alternative_id alternative_id :label label :subClassOf subClassOf :hasDbXref hasDbXref :synonym synonym})))

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
       (map #(assoc % :id (last (str/split (:id %) #"/"))))
       (map #(assoc % :subClassOf (last (str/split (:subClassOf %) #"/"))))
       (map #(assoc % :dbXref_source (kg/create-source (:hasDbXref %) "DOID")))
       (map #(assoc % :hasDbXref (kg/correct-source-id (:hasDbXref %))))
       (map #(assoc % :hasDbXref (str/replace (:hasDbXref %) "." "")))
       (kg/write-csv [:id :alternative_id :label :subClassOf :hasDbXref :dbXref_source :synonym] output_path)))

(defn run [_]
  (let [url "https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/main/src/ontology/doid.owl"
        output "./resources/stage_0_outputs/doid.csv"]
    (get-results url output)))

