(ns knowledge-graph.stage-0.get-ncit-meddra-mapping
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.set :as set]
   [knowledge-graph.module.module :as kg]))

(defn get-results
  [url output-path]
  (->> (client/get url {:as :reader})
       :body
       csv/read-csv
       kg/csv->map
       (map #(select-keys % [:CODE
                             :PT
                             :RELATIONSHIP_TO_TARGET
                             :TARGET_CODE
                             :TARGET_TERM]))
       (map #(set/rename-keys % {:CODE                   :ncit_id
                                 :PT                     :ncit_name
                                 :RELATIONSHIP_TO_TARGET :relationship
                                 :TARGET_CODE            :meddra_code
                                 :TARGET_TERM            :meddra_name}))
       (filter #(some? (:ncit_id %)))
       (kg/write-csv [:ncit_id
                      :ncit_name
                      :relationship
                      :meddra_code
                      :meddra_name] output-path)))
(defn run
  []
  (let [url "https://ncit.nci.nih.gov/ncitbrowser/ajax?action=export_maps_to_mapping&target=MedDRA"
        output-path "./resources/stage_0_outputs/ncit_meddra_mapping.csv"]
    (get-results url output-path)))