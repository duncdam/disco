(ns knowledge-graph.stage-0.get-kegg
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.set :as set]
   [knowledge-graph.module.module :as kg]))

(defn flatten-column
  [m]
  (let [{:keys [parent children]} m]
    (map (fn [x] {:parent parent :children x}) children)))
  
(defn extract-mapping
  [m]
  (->> (map #(set/rename-keys % {:name :parent}) m)
        (map #(assoc % :children (map (fn[x] (:name x)) (:children %))))
        (map #(assoc % :data (flatten-column %)))
        (map #(:data %))
        (apply concat)
        (map #(assoc % :id (first (str/split (:children %) #"\s{1,}"))))
        (map #(assoc % :source_id (:id %)))
        (map #(assoc % :subClassOf (first (str/split (:parent %) #"\s{1,}"))))
        (map #(assoc % :is_yes? (re-matches #"\d{1}\w{1}\d{2}" (:subClassOf %))))
        (map #(assoc % :is_yes_2? (re-matches #"\w{2}\d{2}" (:subClassOf %))))
        (filter #(or (not (str/blank? (:is_yes? %))) (not (str/blank? (:is_yes_2? %)))))))

(defn get-kegg-mapping
  [url]
  (let [file (->> (client/get url {:as :reader})
                  (:body))
        data-depth-2 (->> (json/read file :key-fn keyword)
                          (:children)
                          (mapcat #(:children %))
                          (mapcat #(:children %)))
        result-depth-2 (extract-mapping data-depth-2)
        data-depth-3 (mapcat #(:children %) data-depth-2)
        result-depth-3 (extract-mapping data-depth-3)
        results (concat result-depth-2 result-depth-3)]
    (map #(select-keys % [:source_id :subClassOf]) results)))

(defn get-kegg
  [url]
  (let [file (->> (client/get url {:as :reader})
                  :body)
        data (->> (csv/read-csv file :separator \tab)
                  (cons ["code" "label"])
                  (kg/csv->map)
                  (map #(assoc % :id (str/replace (:code %) #"ds:" "KEGG_")))
                  (map #(assoc % :source_id (str/replace (:code %) #"ds:" ""))))]
    (map #(select-keys % [:id :label :source_id]) data)))

(def url-info "https://rest.kegg.jp/list/disease")
(def url-mapping "https://rest.kegg.jp/get/br:br08403/json")
(def output-path "./resources/stage_0_outputs/kegg.csv")

(defn run [_]
  (let [kegg-info (get-kegg url-info)
        kegg-mapping (get-kegg-mapping url-mapping)]
    (->> (kg/joiner kegg-info kegg-mapping :source_id :source_id kg/left-join)
         (kg/write-csv [:id :label :source_id :subClassOf] output-path))))