(ns knowledge-graph.stage-0.get-meddra
  (:require
   [clj-http.client :as client]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [url output-path]
  (let [meddra (->> (client/get url {:as :reader})
                    :body
                    csv/read-csv
                    kg/csv->map
                    (map #(set/rename-keys % {:TARGET_CODE :id :TARGET_TERM :label }))
                    (map #(assoc % :source_id (:id %)))
                    (map #(assoc % :id (str/join "_" ["MEDDRA" (:id %)]))))]
        (kg/write-csv [:id :label :source_id] output-path meddra)))

(defn run [_]
  (let [url "https://ncit.nci.nih.gov/ncitbrowser/ajax?action=export_maps_to_mapping&target=MedDRA"
        output-path "./resources/stage_0_outputs/meddra.csv"]
        (get-results url output-path)))