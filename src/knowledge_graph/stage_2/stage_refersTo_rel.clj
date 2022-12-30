(ns knowledge-graph.stage-2.stage-refersTo-rel
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [taoensso.timbre :as log]
   [knowledge-graph.module.module :as kg]))

(defn load-dbXref
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (mapv #(select-keys % [:start_id :type :end_id])))))

(defn create-reference-pair
  [hasDbXref ref-key group-key]
  (let [hasDbXref_1 (->> (map #(set/rename-keys % {group-key :start_id ref-key :ref}) hasDbXref)
                         (map #(select-keys % [:start_id :ref])))
        hasDbXref_2 (->> (map #(set/rename-keys % {group-key :end_id ref-key :ref}) hasDbXref)
                         (map #(select-keys % [:end_id :ref])))
        joined_hasDbXref (kg/joiner hasDbXref_1 hasDbXref_2 :ref :ref kg/inner-join)]
    (->> (filter #(not= (:start_id %) (:end_id %)) joined_hasDbXref)
         (group-by (juxt :start_id :end_id)))))

(def file-path "stage_2_outputs/hasDbXref_rel.csv")
(def output-path "./resources/stage_2_outputs/refersTo_rel.csv")
(def hasDbXref (load-dbXref file-path))
(def groupBy (concat (create-reference-pair hasDbXref :end_id :start_id)
                     (create-reference-pair hasDbXref :start_id :end_id)))
(def data-map  (->> (map #(into {} {:start_id (first (first %)) :end_id (second (first %)) :hasDbXref (second %)}) groupBy)
                    (map #(assoc % :hasDbXref (map (fn [x] (into () (:end_id x))) (:hasDbXref %))))
                    (filter #(not (str/blank? (:start_id %))))
                    (filter #(not (str/blank? (:end_id %))))))

(defn run
  [_]
  (let [refersTo-fw (->> (map #(assoc % :start_type (first (str/split (:start_id %) #"_"))) data-map)
                         (map #(assoc % :end_type (first (str/split (:end_id %) #"_"))))
                         ;; make sure two terms are from different ontology for refersTo relationship
                         (filter #(not= (:start_type %) (:end_type %)))
                         (map #(assoc % :number_common_xRef (count (:hasDbXref %))))
                         ;; refersTo between terms have at least 2 common cross reference
                         ;; if between ICD terms, need to be at least 5 common cross reference
                         (filter #(cond
                                    (and (str/includes? (:start_type %) "ICD") (str/includes? (:end_type %) "ICD")) (>= (:number_common_xRef %) 4)
                                    (and (str/includes? (:start_type %) "ICD") (str/includes? (:end_type %) "PHECODE")) (>= (:number_common_xRef %) 4)
                                    (and (str/includes? (:start_type %) "PHECODE") (str/includes? (:end_type %) "ICD")) (>= (:number_common_xRef %) 4)
                                    :else (>= (:number_common_xRef %) 2)))
                         (map #(assoc % :type "refersTo"))
                         (map #(select-keys % [:start_id :type :end_id :number_common_xRef])))
        refersTo-rel  (->> (concat (map #(assoc % :number_common_xRef 0) hasDbXref) refersTo-fw)
                           (group-by (juxt :start_id :end_id))
                           (map #(into {} {:start_id (first (first %)) :end_id (second (first %)) :type (second %)}))
                           (map #(assoc % :type (map (fn [x] (into () (:type x))) (:type %))))
                           (map #(assoc % :type (distinct (:type %))))
                           (map #(assoc % :count (count (:type %))))
                           ;; remove relationships already linked by hasDbXref
                           (filter #(= (:count %) 1))
                           (filter #(= (first (:type %)) "refersTo"))
                           (map #(assoc % :type (first (:type %))))
                           (map #(select-keys % [:start_id :type :end_id])))]
    (kg/write-csv [:start_id :type :end_id  :hasDbXref_start :hasDbXref_end :number_common_xRef] output-path refersTo-rel)
    (log/info "finished staging refersTo relationships")))