(ns knowledge-graph.stage-4.disco
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.math :as m]
   [clojure.data.csv :as csv]
   [taoensso.timbre :as log]
   [knowledge-graph.module.module :as kg])
  (:import (java.lang Integer)
           (java.lang Float)))

(defn load-csv-file
  [file-path keys]
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \tab)
         kg/csv->map
         (mapv #(select-keys % keys)))))

(def stopwords
  "Not a complete list of english stop words but good enough for this example"
  #{"i" "me" "my" "myself" "we" "our" "ours" "ourselves" "you"
    "your" "yours" "yourself" "yourselves" "he" "him" "his"
    "himself" "she" "her" "hers" "herself" "it" "its" "itself"
    "they" "them" "their" "theirs" "themselves" "what" "which"
    "who" "this" "that" "these" "those" "the" "a" "an"
    "am" "is" "are" "was" "were" "be" "been" "being"
    "have" "has" "had" "having"
    "(finding)" "(disorder)" "(procedure)" "(situation)"})

(defn split-string
  "function to split strings into collection"
  [s]
  (-> (str/lower-case s)
      (str/split #" ")))

(defn calculate-magnitute
  "calculate magniture of the vector"
  [v]
  (->> (map #(* % %) v)
       (reduce +)
       m/sqrt))

(defn dot-product
  "vectors dot product"
  [vector-a vector-b]
  (->> (map * vector-a vector-b)
       (reduce +)))

(defn cosine
  "calculating cosine value of two vectors"
  [string-a string-b]
  (let [token-a (remove stopwords (split-string string-a))
        token-b (remove stopwords (split-string string-b))
        all-tokens (distinct (concat token-a token-b))
        vector-a (map #(get (frequencies token-a) % 0) all-tokens)
        vector-b (map #(get (frequencies token-b) % 0) all-tokens)]
    (Float/parseFloat (format "%.2f" (/ (dot-product vector-a vector-b)
                                        (* (calculate-magnitute vector-a) (calculate-magnitute vector-b)))))))

(defn create-disco-codes
  "Create DISCO terms for duplicated terms from different disease ontology"
  [file-path-dbXref file-path-relatedTo file-path-prefLabel file-path-synonyms file-path-diseases file-path-disco-cached]
  (let [dbXref (load-csv-file file-path-dbXref [:start_id :type :end_id])
        relatedTo (load-csv-file file-path-relatedTo [:start_id :type :end_id])
        prefLabel (->> (load-csv-file file-path-prefLabel [:start_id :end_id])
                       (group-by :start_id)
                       (map #(into {} {:start_id (first %)
                                       :end_id (first (sort (map (fn [x] (:end_id x)) (second %))))})))
        synonyms (load-csv-file file-path-synonyms [:id :name])
        diseases (load-csv-file file-path-diseases [:id])
        cached-disco (if (.exists (io/file file-path-disco-cached))
                       (load-csv-file file-path-disco-cached [:id :hash_string])
                       (list (hash-map)))
        rel (concat dbXref relatedTo)
        related-name (-> (kg/joiner rel
                                    (map #(set/rename-keys % {:end_id :start_prefLabel_id}) prefLabel)
                                    :start_id :start_id
                                    kg/inner-join)
                         (kg/joiner (map #(set/rename-keys % {:end_id :end_prefLabel_id :start_id :end_id}) prefLabel)
                                    :end_id :end_id
                                    kg/inner-join)
                         (kg/joiner (map #(set/rename-keys % {:id :start_prefLabel_id :name :start_name}) synonyms)
                                    :start_prefLabel_id :start_prefLabel_id
                                    kg/inner-join)
                         (kg/joiner (map #(set/rename-keys % {:id :end_prefLabel_id :name :end_name}) synonyms)
                                    :end_prefLabel_id :end_prefLabel_id
                                    kg/inner-join))
        related-disco  (->> related-name
                            (filter #(or (= (:type %) "relatedTo")
                                         (str/includes? (:start_id %) "PHECODE")
                                         (str/includes? (:end_id %) "PHECODE")
                                         (str/includes? (:start_id %) "ICD")
                                         (str/includes? (:end_id %) "ICD")))
                            (map #(assoc % :string_similarity (cosine (:start_name %) (:end_name %))))
                            (filter #(>= (:string_similarity %) 0.75))
                            (map #(select-keys % [:start_id :start_name :end_id :end_name])))
        dbXref-disco (->> related-name
                          (filter #(and (= (:type %) "hasDbXref")
                                        (not (str/includes? (:start_id %) "PHECODE"))
                                        (not (str/includes? (:end_id %) "PHECODE"))
                                        (not (str/includes? (:start_id %) "ICD"))
                                        (not (str/includes? (:end_id %) "ICD"))))
                          (map #(select-keys % [:start_id :start_name :end_id :end_name])))
        xref-disco (->> (concat related-disco dbXref-disco)
                        (group-by (juxt :start_id))
                        (map #(into {} {:name (str/lower-case (first (sort (distinct (map (fn [x] (:start_name x)) (second %))))))
                                        :synonyms (map (fn [x] (:end_name x)) (second %))
                                        :all (sort (distinct (conj (map (fn [x] (:end_id x)) (second %)) (first (first %)))))}))
                        (map #(assoc % :name (remove #{"(finding)" "(disorder)" "(procedure)" "(situation)"} (str/split (:name %) #" "))))
                        (map #(assoc % :name (str/join " " (:name %))))
                        distinct
                        (group-by :name)
                        (map #(into {} {:name (first %)
                                        :dbXref (sort (distinct (apply concat (map (fn [x] (:all x)) (second %)))))
                                        :synonyms (apply concat (map (fn [x] (:synonyms x)) (second %)))}))
                        (group-by :dbXref)
                        (map #(into {} {:dbXref (first %)
                                        :name (first (sort (map (fn [x] (:name x)) (second %))))
                                        :synonyms (apply concat (map (fn [x] (:synonyms x)) (second %)))}))
                        (map #(assoc % :synonyms (str/join ";" (distinct (:synonyms %)))))
                        distinct)
        processed-xref-disco (->> xref-disco
                                  (map #(map (fn [x] (into {} {:id x})) (:dbXref %)))
                                  (apply concat)
                                  distinct
                                  (map #(assoc % :is_disco "yes")))
        nonxref-disco (->> (kg/joiner diseases processed-xref-disco :id :id kg/left-join)
                           (filter #(str/blank? (:is_disco %)))
                           distinct)
        nonxref-disco-name (->> (-> (kg/joiner nonxref-disco
                                               (map #(set/rename-keys % {:start_id :id :end_id :prefLabel_id}) prefLabel)
                                               :id :id
                                               kg/inner-join)
                                    (kg/joiner (map #(set/rename-keys % {:id :prefLabel_id}) synonyms)
                                               :prefLabel_id :prefLabel_id
                                               kg/inner-join))
                                (map #(assoc % :dbXref (:id %)))
                                (map #(assoc % :name (str/lower-case (:name %))))
                                (map #(select-keys % [:name :dbXref]))
                                (group-by :name)
                                (map #(into {:name (first %)
                                             :dbXref (map (fn [x] (:dbXref x)) (second %))
                                             :synonyms (map (fn [x] (:name x)) (second %))}))
                                (map #(assoc % :name (remove #{"(finding)" "(disorder)" "(procedure)" "(situation)"} (str/split (:name %) #" "))))
                                (map #(assoc % :name (str/join " " (:name %))))
                                (map #(assoc % :synonyms (str/join ";" (distinct (:synonyms %)))))
                                distinct)
        processed-disco (->> (concat xref-disco nonxref-disco-name)
                             (map #(assoc % :xref_string (map (fn [x] (str/split x #"_")) (:dbXref %))))
                             (map #(assoc % :xref_string (map (fn [x] (second x)) (:xref_string %))))
                             (map #(assoc % :xref_string (distinct (:xref_string %))))
                             (map #(assoc % :xref_string (str/join "" (apply concat (:xref_string %)))))
                             (map #(assoc % :name_string (str/split (:name %) #" ")))
                             (map #(assoc % :name_string (map (fn [x] (str/split x #"")) (:name_string %))))
                             (map #(assoc % :name_string (str/join "" (apply concat (:name_string %)))))
                             (map #(assoc % :hash_string (str/join "" [(:xref_string %) (:name_string %)])))
                             (map #(select-keys % [:name :dbXref :hash_string :synonyms]))
                             distinct)
        evaluate?-disco (kg/joiner processed-disco cached-disco :hash_string :hash_string kg/left-join)
        old-disco (filter #(not (str/blank? (:id %))) evaluate?-disco)
        last-id (if (> (count old-disco) 0)
                  (-> (map #(:id %) old-disco)
                      sort
                      last
                      (str/split #"_")
                      last
                      Integer/parseInt)
                  0)
        new-disco (->> (filter #(str/blank? (:id %)) evaluate?-disco)
                       (sort-by :hash_string)
                       (map-indexed vector)
                       (map #(into (second %) {:id (str/join "_" ["DISCO" (format "%07d" (+ (first %) last-id))])})))
        disco (concat old-disco new-disco)]
    disco))

(defn disco-refersTo
  [disco]
  (let [{:keys [id dbXref]} disco]
    (->> (map (fn [x] {:start_id id :end_id x}) dbXref)
         (map #(assoc % :type "refersTo")))))

(def file-path-dbXref "./stage_2_outputs/hasDbXref_rel.csv")
(def file-path-relatedTo "./stage_2_outputs/relatedTo_rel.csv")
(def file-path-prefLabel "./stage_2_outputs/prefLabel_rel.csv")
(def file-path-synonyms "./stage_1_outputs/synonym_nodes.csv")
(def file-path-diseases "./stage_1_outputs/disease_nodes.csv")
(def file-path-disco-cached "./stage_4_outputs/cached_disco.csv")
(def disco (create-disco-codes
            file-path-dbXref file-path-relatedTo
            file-path-prefLabel file-path-synonyms
            file-path-diseases file-path-disco-cached))

(defn run
  [_]
  (let [cached-disco (distinct (map #(select-keys % [:id :hash_string]) disco))
        final-disco (->> (map #(select-keys % [:id :name :synonyms]) disco)
                         (map #(assoc % :label "DISCO"))
                         (map #(assoc % :source_id (:id %)))
                         (map #(assoc % :source "DISCO"))
                         distinct)
        refersTo-disco (->> (map #(disco-refersTo  %) disco)
                            (apply concat)
                            distinct)]
    (log/info "Saving hashstring and previous DISCO ids")
    (kg/write-csv [:id :hash_string] "./resources/stage_4_outputs/cached_disco.csv" cached-disco)
    (log/info "Saving new version of DISCO")
    (->> final-disco
         (map #(set/rename-keys % {:id :ID :label :LABEL :synonyms "synonyms:string[]"}))
         (kg/write-csv [:LABEL :ID :name :source_id :source "synonyms:string[]"] "./resources/stage_4_outputs/disco.csv"))
    (log/info "Saving refersTo relationship of DISCO terms")
    (->> refersTo-disco
         (map #(set/rename-keys % {:start_id :START_ID :type :TYPE :end_id :END_ID}))
         (kg/write-csv [:START_ID :TYPE :END_ID] "./resources/stage_4_outputs/refersTo_rel.csv"))))