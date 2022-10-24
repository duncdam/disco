(ns knowledge-graph.stage-0.get-kegg
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [clojure.set :as set]
   [knowledge-graph.module.module :as kg]))

(defn get-kegg
  [url output-path]
  (let [file (->> (client/get url {:as :reader})
                  :body)
        data (->> (csv/read-csv file :separator \tab)
                  (cons ["code" "label"])
                  (kg/csv->map)
                  (map #(assoc % :id (str/replace (:code %) #"ds:" "KEGG:")))
                  (map #(set/rename-keys % {:code :source_id})))]
       (kg/write-csv [:id :label :source_id] output-path data)))

(defn run [_]
  (let [url "https://rest.kegg.jp/list/disease"
        output-path "./resources/stage_0_outputs/kegg.csv"]
    (get-kegg url output-path)))