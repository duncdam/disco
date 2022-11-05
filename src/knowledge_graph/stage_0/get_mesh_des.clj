(ns knowledge-graph.stage-0.get-mesh-des
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [text xml-> ]]
   [clojure.string :as str]
   [clojure.set :as set]
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
  (let [mesh-data (->> (client/get url {:as :stream})
                       :body
                       (d-xml/parse)
                       :content
                       (filter #(= (:tag %) :DescriptorRecord))
                       (map class-map)
                       (apply concat)
                       (filter #(some? (:id %)))
                       (filter #(str/includes? (:tree_location %) "C"))
                       distinct)
        mesh-parent (->> (map #(assoc % :parent_tree_location (str/split (:tree_location %) #"\.")) mesh-data)
                         (map #(assoc % :parent_tree_location (drop-last (:parent_tree_location %))))
                         (map #(assoc % :parent_tree_location (str/join "." (:parent_tree_location %))))
                         (map #(set/rename-keys % {:id :subClassOf}))
                         (map #(select-keys % [:subClassOf :parent_tree_location])))
        mesh-des (kg/joiner mesh-data mesh-parent :tree_location :parent_tree_location kg/left-join)]
        (->> (map #(assoc % :source_id (:id %)) mesh-des)
             (map #(assoc % :id (str/join "_" ["MESH" (:id %)])))
             (map #(select-keys % [:id :label :source_id :synonym :subClassOf]))
             distinct 
             (kg/write-csv [:id :label :source_id :synonym :subClassOf] output_path))))

(defn run [_]
  (let [url "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/desc2022.xml"
        output "./resources/stage_0_outputs/mesh_descriptor.csv"]
    (get-results url output)))


