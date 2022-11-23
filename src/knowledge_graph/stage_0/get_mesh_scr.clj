(ns knowledge-graph.stage-0.get-mesh-scr
  (:require
   [clj-http.client :as client]
   [clojure.java.io :as io]
   [clojure.data.xml :as d-xml]
   [clojure.data.zip.xml :refer [text xml->]]
   [clojure.string :as str]
   [clojure.zip :as z]
   [clojure.data.csv :as csv]
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
  [scr-path mesh-desc-path output_path]
  (with-open [des-file (io/reader (io/resource mesh-desc-path))
              scr-file (io/reader (io/resource scr-path))]
    (let [mesh-scr (->> (d-xml/parse scr-file)
                        :content
                        (filter #(= (:tag %) :SupplementalRecord))
                        (map class-map)
                        (apply concat)
                        (filter #(some? (:id %)))
                        distinct)
          mesh-desc (->> (csv/read-csv des-file :separator \tab)
                         kg/csv->map
                         (map #(select-keys % [:source_id]))
                         distinct)
          mesh-scr-disease (kg/joiner mesh-scr mesh-desc :subClassOf :source_id kg/inner-join)]
      (->> (map #(assoc % :source_id (:id %)) mesh-scr-disease)
           (map #(assoc % :hasDbXref ""))
           (map #(assoc % :dbXref_source ""))
           (map #(select-keys % [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym]))
           distinct
           (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output_path)))))

(defn run [_]
  (let [scr-file "downloads/supp2022.xml"
        output-path "./resources/stage_0_outputs/mesh_scr.csv"
        mesh-desc-path "stage_0_outputs/mesh_des.csv"]
    (get-results scr-file mesh-desc-path output-path)))

