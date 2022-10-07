(ns knowledge-graph.stage-0.get-ncit
  (:require
   [camel-snake-kebab.core :as csk]
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn csv->map
  [csv-data]
  (map zipmap
        (->> (first csv-data)
            (map #(csk/->camelCase %))
            (map keyword)
            repeat)
        (rest csv-data)))

(defn flatten-synonym
  [m]
  (let [{:keys [id label synonym]} m]
    (map (fn [x] {:id id :label label :synonym x}) synonym)))


(defn get-ncit
  [url output-path]
  (let [ncit-raw (->> (client/get url {:as :reader})
                  :body
                  csv/read-csv 
                  csv->map
                  (map #(assoc % :id (:code %)))
                  (map #(assoc % :label (:preferredTerm %)))
                  (map #(assoc % :synonym (:synonyms %)))
                  (map #(assoc % :synonym (str/split (:synonym %) #"\|")))
                  (map #(select-keys % [:id :label :synonym])))
        ncit (->> (map #(flatten-synonym %) ncit-raw)
                  (apply concat))]
        (kg/write-csv [:id :label :synonym] output-path ncit)))

(defn run [_]
  (let [url "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Neoplasm/Neoplasm_Core.csv"
        output-path "./resources/stage_0_outputs/ncit.csv"]
    (get-ncit url output-path)))

