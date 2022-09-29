(ns knowledge-graph.stage-0.parse-orphanet
  (:require
   [clj-http.client :as client]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [text xml-> xml1->]]
   [clojure.string :as str]
   [clojure.zip :as z]
   [knowledge-graph.module.module :as kg]))

(defn orphanet-map
  [data]
  (let [z (z/xml-zip data)]
    (for [id (xml-> z :Disorder :OrphaCode text)
          name (xml-> z :Disorder :Name text)      
          s (zipmap (xml-> z :Disorder :ExternalReferenceList :ExternalReference :Source text)
                    (xml-> z :Disorder :ExternalReferenceList :ExternalReference :Reference text))
          synonym (xml-> z :Disorder :SynonymList :Synonym text)]
      {:id id :name name :synonym synonym :ref_source (first s) :ref_source_id (second s)})))

(defn get-orphanet
  [url output-path]
  (->>
   (client/get url {:as :stream})
   :body
   d-xml/parse
   :content
   (filter #(= (:tag %) :DisorderList))
   first
   :content
   (filter #(= (:tag %) :Disorder))
   (map orphanet-map)
   (apply concat)
   distinct
   (kg/write-csv [:id :name :synonym :ref_source :ref_source_id] output-path)))

(defn run[]
  (let [url "http://www.orphadata.com/data/xml/en_product1.xml"
        output-path-orphanet "./resources/stage_0_outputs/orphanet.csv"]
    (get-orphanet url output-path-orphanet)))