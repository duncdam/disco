(ns knowledge-graph.stage-0.get-kegg-mapping
  (:require
   [clj-http.client :as client]
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
       (map #(assoc % :hasDbXref (first (str/split (:parent %) #"\s{1,}"))))
       (map #(assoc % :is_yes? (re-matches #"\d{1}\w{1}\d{2}" (:hasDbXref %))))
       (map #(assoc % :is_yes_2? (re-matches #"\w{2}\d{2}" (:hasDbXref %))))
       (filter #(or (not (str/blank? (:is_yes? %))) (not (str/blank? (:is_yes_2? %)))))
       (map #(assoc % :hasDbXref_source "ICD11CM"))))

(defn get-kegg-mapping
  [url output-path]
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
      (kg/write-csv [:id :hasDbXref :hasDbXref_source] output-path results)
    ))

(defn run [_]
  (let [url "https://rest.kegg.jp/get/br:br08403/json"
        output-path "./resources/stage_0_outputs/kegg_mapping.csv"]
      (get-kegg-mapping url output-path)))