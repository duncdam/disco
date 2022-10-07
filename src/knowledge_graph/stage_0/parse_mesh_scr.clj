(ns knowledge-graph.stage-0.parse-mesh-scr
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [text xml->]]
   [clojure.string :as str]
   [clojure.zip :as z]
   [knowledge-graph.module.module :as kg]))

(defn class-map
  "Extract information from xml elements and create a map"
  [data]
  (let [z (z/xml-zip data)]
    (for [id      (xml-> z :SupplementalRecordUI text)
          label   (xml-> z :SupplementalRecordName text)
          synonym (xml-> z :ConceptList :Concept :TermList :Term :String text)
          subClassOf (xml-> z :HeadingMappedToList :HeadingMappedTo :DescriptorReferredTo :DescriptorUI text)]
      {:id id :label label :synonym synonym :subClassOf (str/replace subClassOf "*" "")})))

(defn get-results
  "Stream xml file, parse for necessary information, and write as csv output"
  [url output_path]
  (->>
   (client/get url {:as :stream})
   :body
   (d-xml/parse)
   :content
   (filter #(= (:tag %) :SupplementalRecord))
   (map class-map)
   (apply concat)
   (filter #(some? (:id %)))
   distinct
   (kg/write-csv [:id :label :synonym :subClassOf]  output_path)))

(defn run [_]
  (let [url "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/supp2022.xml"
        output "./resources/stage_0_outputs/mesh_scr.csv"]
    (get-results url output)))

