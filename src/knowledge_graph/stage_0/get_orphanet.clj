(ns knowledge-graph.stage-0.get-orphanet
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
          label (xml-> z :Class :label text)
          subClassOf (xml-> z :Class :subClassOf (attr :rdf/resource))
          hasDbXref (xml-> z :Class :hasDbXref text)
          synonym (xml-> z :Class :alternative_term text)
          source_id (xml-> z :Class :notation text)]
      {:id id :label label :subClassOf subClassOf :hasDbXref hasDbXref :synonym synonym :source_id source_id})))

(defn get-orphanet
  [url output-path]
  (->> (client/get url {:as :stream})
       :body
       d-xml/parse
       :content
       (filter #(= (:tag %) :Class))
       (map orphanet-map)
       (apply concat)
       (filter #(not= (str/lower-case (:source_id %)) "clinical subtype"))
       (map #(assoc % :id (str/upper-case (last (str/split (:id %) #"/")))))
       (map #(assoc % :subClassOf (str/upper-case (last (str/split (:subClassOf %) #"/")))))
       (map #(assoc % :dbXref_source (first (str/split (:hasDbXref %) #":"))))
       (map #(assoc % :dbXref_source (kg/correct-source (:dbXref_source %))))
       (map #(assoc % :hasDbXref (kg/correct-xref-id (:hasDbXref %))))
       (map #(assoc % :hasDbXref (str/replace (:hasDbXref %) "." "")))
       (map #(select-keys % [:id :label :subClassOf :hasDbXref :synonym :dbXref_source :source_id]))
       distinct
       (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path)
      ))

(defn run [_]
  (let [url "https://www.orphadata.com/data/ontologies/ordo/last_version/ORDO_en_4.1.owl"
        output-path-orphanet "./resources/stage_0_outputs/orphanet.csv"]
    (get-orphanet url output-path-orphanet)))