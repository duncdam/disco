(ns knowledge-graph.stage-2.stage-relatedTo-rel
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

(defn flatten-column
  [m]
  (let [{:keys [start_id end_id]} m]
    (map (fn [x] {:start_id start_id :end_id x}) end_id)))

(defn create-reference-pair
  [hasDbXref ref-key group-key]
  (let [hasDbXref (->> hasDbXref
                       (filter #(not (or (and (str/includes? (:start_id %) "MEDGEN") (str/includes? (:end_id %) "UMLS"))
                                         (and (str/includes? (:start_id %) "UMLS") (str/includes? (:end_id %) "MEDGEN")))))
                       (group-by :start_id)
                       (map #(into {} {:start_id (first %)
                                       :end_id (map (fn [x] (:end_id x)) (second %))
                                       :type (map (fn [x] (first (str/split (:end_id x) #"_"))) (second %))}))
                       (filter #(= (count (:type %)) (count (distinct (:type %)))))
                       (map #(flatten-column %))
                       (apply concat))
        hasDbXref_1 (->> (map #(set/rename-keys % {group-key :start_id ref-key :ref}) hasDbXref)
                         (map #(select-keys % [:start_id :ref])))
        hasDbXref_2 (->> (map #(set/rename-keys % {group-key :end_id ref-key :ref}) hasDbXref)
                         (map #(select-keys % [:end_id :ref])))
        joined_hasDbXref (kg/joiner hasDbXref_1 hasDbXref_2 :ref :ref kg/inner-join)]
    (->> (filter #(not= (:start_id %) (:end_id %)) joined_hasDbXref)
         (group-by (juxt :start_id :end_id)))))

(def file-path "stage_2_outputs/hasDbXref_rel.csv")
(def output-path "./resources/stage_2_outputs/relatedTo_rel.csv")
(def hasDbXref (load-dbXref file-path))
(def groupBy (concat (create-reference-pair hasDbXref :end_id :start_id)
                     (create-reference-pair hasDbXref :start_id :end_id)))
(def data-map  (->> (map #(into {} {:start_id (first (first %)) :end_id (second (first %)) :hasDbXref (second %)}) groupBy)
                    (map #(assoc % :hasDbXref (distinct (map (fn [x] (:end_id x)) (:hasDbXref %)))))
                    (filter #(not (str/blank? (:start_id %))))
                    (filter #(not (str/blank? (:end_id %))))))

(defn run
  [_]
  (let [relatedTo-fw (->> (map #(assoc % :start_type (first (str/split (:start_id %) #"_"))) data-map)
                          (map #(assoc % :end_type (first (str/split (:end_id %) #"_"))))
                         ;; make sure two terms are from different ontology for relatedTo relationship
                          (filter #(not= (:start_type %) (:end_type %)))
                          (map #(assoc % :number_common_xRef (count (:hasDbXref %))))
                          (map #(assoc % :type "relatedTo"))
                          (map #(select-keys % [:start_id :type :end_id :number_common_xRef])))
        relatedTo-rel  (->> (concat (map #(assoc % :number_common_xRef 0) hasDbXref) relatedTo-fw)
                            (group-by (juxt :start_id :end_id))
                            (map #(into {} {:start_id (first (first %)) :end_id (second (first %)) :type (second %)}))
                            (map #(assoc % :type (distinct (map (fn [x] (:type x)) (:type %)))))
                            (map #(assoc % :count (count (:type %))))
                           ;; remove relationships already linked by hasDbXref
                            (filter #(= (:count %) 1))
                            (filter #(= (first (:type %)) "relatedTo"))
                            (filter #(not (or (and (str/includes? (:start_id %) "ICD") (str/includes? (:end_id %) "ICD"))
                                              (and (str/includes? (:start_id %) "ICD") (str/includes? (:end_id %) "PHECODE"))
                                              (and (str/includes? (:start_id %) "PHECODE") (str/includes? (:end_id %) "ICD"))
                                              (and (str/includes? (:start_id %) "SNOMEDCT") (str/includes? (:end_id %) "ICD"))
                                              (and (str/includes? (:start_id %) "ICD") (str/includes? (:end_id %) "SNOMEDCT")))))
                            (map #(assoc % :type (first (:type %))))
                            (map #(select-keys % [:start_id :type :end_id])))]
    (kg/write-csv [:start_id :type :end_id] output-path relatedTo-rel)))