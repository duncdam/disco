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

(defn get-kegg-subClassOf
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

(defn get-kegg-disease
  [url-disease url-dp-mapping]
  (let [disease-file (->> (client/get url-disease {:as :reader})
                          :body)
        disease-data (->> (csv/read-csv disease-file :separator \tab)
                          (cons ["code" "label"])
                          (kg/csv->map)
                          (map #(assoc % :id (str/replace (:code %) #"ds:" "KEGG_")))
                          (map #(assoc % :source_id (str/replace (:code %) #"ds:" ""))))
        disease-mapping-file (->> (client/get url-dp-mapping {:as :reader})
                                  :body)
        disease-mapping (->> (csv/read-csv disease-mapping-file :separator \tab)
                             (cons ["pathway_id" "disease_id"])
                             kg/csv->map
                             (map #(assoc % :id (str/replace (:disease_id %) #"ds:" "KEGG_")))
                             (map #(assoc % :hasDbXref (str/replace (:pathway_id %) #"path:hsa" ""))))]
    (->> (kg/joiner disease-data disease-mapping :id :id kg/left-join )
         (map #(select-keys % [:id :label :source_id :hasDbXref])))))

(defn get-kegg-pathway-disease
  [url-pathway url-dp-mapping]
  (let [pathway-info-file (->> (client/get url-pathway {:as :read})
                          :body)
        pathway-info (->> (csv/read-csv pathway-info-file :separator \tab)
                          (cons ["code" "label"])
                          (kg/csv->map)
                          (map #(assoc % :id (str/replace (:code %) #"path:hsa" "KEGG_" )))
                          (map #(assoc % :source_id (str/replace (:code %) #"path:hsa" "")))
                          (map #(assoc % :label (str/replace (:label %) " - Homo sapiens (human)" "")))
                          (map #(select-keys % [:id :label :source_id])))
        dp-mapping-file (->> (client/get url-dp-mapping {:as :read})
                             :body) 
        dp-mapping  (->> (csv/read-csv dp-mapping-file :separator \tab) 
                         (cons ["pathway_id" "disease_id"])
                         kg/csv->map
                         (map #(assoc % :id (str/replace (:pathway_id %) #"path:hsa" "KEGG_" )))
                         (map #(assoc % :hasDbXref (str/replace (:disease_id %) #"ds:" "" ))))]
    (->> (kg/joiner pathway-info dp-mapping :id :id kg/inner-join)
         (map #(select-keys % [:id :label :source_id :hasDbXref])))))

(def url-info "https://rest.kegg.jp/list/disease")
(def url-subClassOf "https://rest.kegg.jp/get/br:br08403/json")
(def url-pathway "https://rest.kegg.jp/list/pathway/hsa")
(def url-dp-mapping "https://rest.kegg.jp/link/disease/pathway")
(def output-path "./resources/stage_0_outputs/kegg.csv")

(defn run [_]
  (let [kegg-disease (get-kegg-disease url-info url-dp-mapping)
        kegg-disease-pathway (get-kegg-pathway-disease url-pathway url-dp-mapping)
        kegg (concat kegg-disease kegg-disease-pathway)
        kegg-mapping (get-kegg-subClassOf url-subClassOf)]
    (->> (kg/joiner kegg kegg-mapping :source_id :source_id kg/left-join)
         (map #(assoc % :dbXref_source "KEGG"))
         (map #(assoc % :synonym ""))
         (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path))))