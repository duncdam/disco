(ns knowledge-graph.stage-0.get-icd11
  (:require 
    [dk.ative.docjure.spreadsheet :as docjure]
    [clojure.string :as str]
    [knowledge-graph.module.module :as kg]))

(defn get-icd11 
  [file-path sheet]
  (->> (docjure/load-workbook-from-resource file-path)
       (docjure/select-sheet sheet)
       (docjure/select-columns {:C :id :E :label})
       (rest)
       (filter #(not (str/blank? (:id %))))
       (map #(assoc % :label (str/replace (:label %) #"\-\s" "")))
       (map #(assoc % :source_id (str/replace (:id %), #"\." "")))
       (map #(assoc % :subClassOf (first (str/split (:id %) #"\."))))
       (map #(assoc % :subClassOf (cond 
        (not= (:subClassOf %) (:id %)) (:subClassOf %)
        :else nil)))
       (map #(assoc % :id (str/replace (:id %) #"\." "")))
       (map #(select-keys % [:id :label :source_id :subClassOf]))))

(defn get-icd11-mapping
  [file-path sheet]
  (->> (docjure/load-workbook-from-resource file-path)
       (docjure/select-sheet sheet)
       (docjure/select-columns {:B :source_id :E :hasDbXref})
       (rest)
       (filter #(not (str/blank? (:source_id %))))
       (map #(assoc % :dbXref_source "ICD10CM"))
       (map #(select-keys % [:source_id :hasDbXref :dbXref_source]))))

(def file-path-info "./downloads/LinearizationMiniOutput-MMS-en.xlsx")
(def sheet-info "LinearizationMiniOutput-MMS-en")
(def file-path-mapping "./downloads/11To10MapToOneCategory.xlsx")
(def sheet-mapping "11To10MapToOneCategory")
(def output-path "./resources/stage_0_outputs/icd11.csv")

(defn run [_]
  (let [icd11-info (get-icd11 file-path-info sheet-info)
        icd11-mapping (get-icd11-mapping file-path-mapping sheet-mapping)]
      (->> (kg/joiner icd11-info icd11-mapping :source_id :source_id kg/left-join)
           (map #(assoc % :synonym ""))
           (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path))
    ))
  