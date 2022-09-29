(ns knowledge-graph.stage-0.parse-mesh-descriptor
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [text xml-> ]]
   [clojure.string :as str]
   [clojure.zip :as z]
   [knowledge-graph.module.module :as kg]))

(defn class-map
  "Extract information from xml elements and create a map"
  [data]
  (let [z (z/xml-zip data)]  
    (for [id      (xml-> z :DescriptorUI text)
          label   (xml-> z :DescriptorName text)
          synonym (xml-> z :ConceptList :Concept :TermList :Term :String text)
          tree_location (xml-> z :TreeNumberList :TreeNumber text)]
    {:id id :label label :synonym synonym :tree_location tree_location})))

(defn get-results
  "Download xml file, parse for necessary information, and write as csv output"
  [url output_path]
  (->> (client/get url {:as :stream})
       :body
       (d-xml/parse)
       :content
       (filter #(= (:tag %) :DescriptorRecord))
       (map class-map)
       (apply concat)
       (filter #(some? (:id %)))
       (filter #(str/includes? (:tree_location %) "C"))
       distinct
       (kg/write-csv [:id :tree_location :label :synonym] output_path)))

(defn run []
  (let [url "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/desc2022.xml"
        output "./resources/stage_0_outputs/mesh_descriptor.csv"]
    (get-results url output)))


