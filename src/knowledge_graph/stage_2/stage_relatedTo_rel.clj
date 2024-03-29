(ns knowledge-graph.stage-2.stage-relatedTo-rel
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn load-relationship-file
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (mapv #(select-keys % [:start_id :type :end_id])))))

(defn flatten-end-id
  [m]
  (let [{:keys [start_id end_id]} m]
    (map (fn [x] {:start_id start_id :end_id x}) end_id)))

(defn flatten-start-id
  [m]
  (let [{:keys [start_id end_id]} m]
    (map (fn [x] {:start_id x :end_id end_id}) start_id)))

(defn create-reference-pair-via-hasDbXref
  [hasDbXref-file-path ref-key group-key]
  (let [hasDbXref (->> (load-relationship-file hasDbXref-file-path)
                       (group-by :start_id)
                       (map #(into {} {:start_id (first %)
                                       :end_id (map (fn [x] (:end_id x)) (second %))
                                       :type (map (fn [x] (first (str/split (:end_id x) #"_"))) (second %))}))
                       (filter #(= (count (:type %)) (count (distinct (:type %)))))
                       (map #(flatten-end-id %))
                       (apply concat)
                       (group-by :end_id)
                       (map #(into {} {:end_id (first %)
                                       :start_id (map (fn [x] (:start_id x)) (second %))
                                       :type (map (fn [x] (first (str/split (:start_id x) #"_"))) (second %))}))
                       (filter #(= (count (:type %)) (count (distinct (:type %)))))
                       (map #(flatten-start-id %))
                       (apply concat))
        hasDbXref_1 (->> (map #(set/rename-keys % {group-key :start_id ref-key :ref}) hasDbXref)
                         (map #(select-keys % [:start_id :ref])))
        hasDbXref_2 (->> (map #(set/rename-keys % {group-key :end_id ref-key :ref}) hasDbXref)
                         (map #(select-keys % [:end_id :ref])))
        joined_hasDbXref (->> (kg/joiner hasDbXref_1 hasDbXref_2 :ref :ref kg/inner-join)
                              (filter #(not= (:start_id %) (:end_id %)))
                              (filter #(not= (second (str/split (:start_id %) #"_")) (second (str/split (:end_id %) #"_")))))]
    joined_hasDbXref))

(defn create-reference-pair-via-prefLabel
  [prefLabel-file-path]
  (let [prefLabel (load-relationship-file prefLabel-file-path)
        prefLabel-1 (->> prefLabel 
                         (map #(set/rename-keys % {:end_id :ref}))
                         (map #(select-keys % [:start_id :ref])))
        prefLabel-2 (->> prefLabel
                         (map #(set/rename-keys % {:end_id :ref :start_id :end_id}))
                         (map #(select-keys % [:ref :end_id])))
        joined_prefLabel (->> (kg/joiner prefLabel-1 prefLabel-2 :ref :ref kg/inner-join)
                              (filter #(not= (:start_id %) (:end_id %)))
                              (filter #(not= (second (str/split (:start_id %) #"_")) (second (str/split (:end_id %) #"_"))))
                              (map #(select-keys % [:start_id :end_id])))]
    joined_prefLabel))

(def hasDbXref-file-path "stage_2_outputs/hasDbXref_rel.csv")
(def prefLabel-file-path "stage_2_outputs/prefLabel_rel.csv")
(def output-path "./resources/stage_2_outputs/relatedTo_rel.csv")

(defn run
  [_]
  (let [data-map (-> (concat (create-reference-pair-via-hasDbXref hasDbXref-file-path :start_id :end_id)
                             (create-reference-pair-via-hasDbXref hasDbXref-file-path :end_id :start_id)
                             (create-reference-pair-via-prefLabel prefLabel-file-path))
                     distinct)
        relatedTo-fw (->> data-map
                          (map #(assoc % :start_type (first (str/split (:start_id %) #"_"))))
                          (map #(assoc % :end_type (first (str/split (:end_id %) #"_"))))
                         ;; make sure two terms are from different ontology for relatedTo relationship
                          (filter #(not= (:start_type %) (:end_type %)))
                          (map #(assoc % :type "relatedTo"))
                          (map #(select-keys % [:start_id :type :end_id])))
        relatedTo-rel  (->> (load-relationship-file hasDbXref-file-path)
                            (concat relatedTo-fw)
                            (group-by (juxt :start_id :end_id))
                            (map #(into {} {:start_id (first (first %)) :end_id (second (first %)) :type (second %)}))
                            (map #(assoc % :type (distinct (map (fn [x] (:type x)) (:type %)))))
                            (map #(assoc % :count (count (:type %))))
                           ;; remove relationships already linked by hasDbXref
                            (filter #(= (:count %) 1))
                            (filter #(= (first (:type %)) "relatedTo"))
                            (map #(assoc % :type (first (:type %))))
                            (map #(select-keys % [:start_id :type :end_id])))]
    (kg/write-csv [:start_id :type :end_id] output-path relatedTo-rel)))