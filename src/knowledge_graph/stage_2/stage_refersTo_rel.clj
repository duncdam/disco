(ns knowledge-graph.stage-2.stage-refersTo-rel
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [knowledge-graph.module.module :as kg]))

(defn load-dbXref
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (mapv #(select-keys % [:start_id :end_id])))))

(def file-path "stage_2_outputs/hasDbXref_rel.csv")
(def output-path "./resources/stage_2_outputs/refersTo_rel.csv")
(def hasDbXref (load-dbXref file-path))
(def groupBy (let [hasDbXref_1 (->> (map #(set/rename-keys % {:start_id :start_id :end_id :ref}) hasDbXref)
                                    (map #(select-keys % [:start_id :ref])))
                   hasDbXref_2 (->> (map #(set/rename-keys % {:start_id :end_id :end_id :ref}) hasDbXref)
                                    (map #(select-keys % [:end_id :ref])))
                   joined_hasDbXref (kg/joiner hasDbXref_1 hasDbXref_2 :ref :ref kg/inner-join)]
               (->> (filter #(not= (:start_id %) (:end_id %)) joined_hasDbXref)
                    (group-by (juxt :start_id :end_id)))))
(def data-map  (->> (map #(into {} {:start_id (first (first %)) :end_id (second (first %)) :hasDbXref (second %)}) groupBy)
                    (map #(assoc % :hasDbXref (map (fn [x] into () (:end_id x)) (:hasDbXref %))))))

(defn run
  [_]
  (let [hasDbXref (->> (map #(set/rename-keys % {:start_id :hasDbXref_start :end_id :hasDbXref_end}) hasDbXref)
                       (map #(select-keys % [:hasDbXref_start :hasDbXref_end])))
        refersTo-fw (->> (map #(assoc % :start_type (first (str/split (:start_id %) #"_"))) data-map)
                         (map #(assoc % :end_type (first (str/split (:end_id %) #"_"))))
                         ;; make sure two terms are from different ontology for refersTo relationship
                         (filter #(not= (:start_type %) (:end_type %)))
                         ;; refersTo between terms have at least 2 common cross reference
                         ;; if between ICD terms, need to be at least 4 common cross reference
                         (map #(assoc % :number_common_xRef (count (:hasDbXref %))))
                         (filter #(cond
                                    (and (str/includes? (:start_type %) "ICD") (str/includes? (:end_type %) "ICD")) (>= (:number_common_xRef %) 4)
                                    :else (>= (:number_common_xRef %) 2)))
                         (map #(assoc % :type "refersTo"))
                         (map #(select-keys % [:start_id :type :end_id :number_common_xRef])))
        ;; remove relationships already linked by hasDbXref
        refersTo-rel-fw (->> (kg/joiner refersTo-fw hasDbXref :start_id :hasDbXref_start kg/inner-join)
                             (filter #(not= (:end_id %) (:hasDbXref_end %)))
                             (map #(select-keys % [:start_id :type :end_id :number_common_xRef])))
        refersTo-rel-bw (->> (map #(set/rename-keys % {:start_id :end_id :end_id :start_id}) refersTo-rel-fw)
                             (map #(select-keys % [:start_id :type :end_id :number_commnon_xRef])))
        refersTo-rel (->> (concat refersTo-rel-fw refersTo-rel-bw)
                          distinct)]
    (kg/write-csv [:start_id :type :end_id :number_common_xRef] output-path refersTo-rel)))