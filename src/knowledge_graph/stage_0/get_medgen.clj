(ns knowledge-graph.stage-0.get-medgen
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [knowledge-graph.module.module :as kg]))

(defn download-file
  [url save-path fname]
  (-> (client/get url {:as :stream})
      :body
      (java.util.zip.GZIPInputStream.)
      (io/copy (io/file save-path fname))))

(defn get-results
  [file-path-concept file-path-name]
  (with-open [concept-file (io/reader (io/resource file-path-concept))
              name-file (io/reader (io/resource file-path-name))]
    (let [concept-syn (->> (slurp concept-file)
                           str/split-lines
                           (map #(str/split % #"\|"))
                           (cons ["CUI" "TS" "STT" "ISPREF" "AUI" "SAUI" "SCUI" "SDUI" "SAB" "TTY" "CODE" "STR" "SUPPRESS"])
                           kg/csv->map
                           (filter #(not= (:CUI %) "#CUI"))
                           (map #(assoc % :hasDbXref (cond
                                                       (or (= (:SAB %) "MONDO")
                                                           (= (:SAB %) "HPO")
                                                           (= (:SAB %) "ORDO")) (:SDUI %)
                                                       :else (:CODE %))))
                           (map #(set/rename-keys % {:CUI :id :STR :synonym}))
                           (map #(assoc % :dbXref_source (kg/correct-source (:SAB %))))
                           (map #(assoc % :hasDbXref (kg/correct-xref-id (:hasDbXref %))))
                           (map #(select-keys % [:id :synonym :hasDbXref :dbXref_source])))
          name-map (->> (slurp name-file)
                        str/split-lines
                        (map #(str/split % #"\|"))
                        (cons ["CUI" "name" "source" "SUPPRESS"])
                        kg/csv->map
                        (filter #(not= (:CUI %) "#CUI"))
                        (map #(set/rename-keys % {:CUI :id :name :label}))
                        (map #(select-keys % [:id :label])))
          medgen (kg/joiner name-map concept-syn :id :id kg/inner-join)]
      (->> (mapv #(select-keys % [:id :label :hasDbXref :dbXref_source :synonym]) medgen)
           distinct))))

(defn get-result-mapping
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data-map (->> (csv/read-csv file :separator \|)
                        kg/csv->map)
          medgen-mapping (->> (map #(set/rename-keys % {:#CUI :id :source_id :hasDbXref :source :dbXref_source}) data-map)
                              (map #(assoc % :dbXref_source (kg/correct-source (:dbXref_source %))))
                              (map #(assoc % :hasDbXref (kg/correct-xref-id (:hasDbXref %))))
                              (map #(assoc % :hasDbXref (str/upper-case (:hasDbXref %))))
                              (mapv #(select-keys % [:id :hasDbXref :dbXref_source])))
          medgen-umls (->>  (map #(assoc % :dbXref_source "UMLS") data-map)
                            (map #(assoc % :hasDbXref (:#CUI %)))
                            (mapv #(select-keys % [:id :hasDbXref :dbXref_source]))
                            distinct)
          mapping (->> (concat medgen-mapping  medgen-umls)
                       distinct)]
      (->> (filter #(some? (:id %)) mapping)
           distinct))))

(defn file-path
  [save-path fname]
  (-> (str save-path fname)
      (str/replace #"/resources" "")))

(def info-url "https://ftp.ncbi.nlm.nih.gov/pub/medgen/")
(def mapping-url "https://ftp.ncbi.nlm.nih.gov/pub/medgen/MedGenIDMappings.txt.gz")
(def fname-concept "MGCONSO.RRF")
(def fname-name "NAMES.RRF")
(def fname-mapping "medgen_id_mapping.txt")
(def concept-url (str info-url fname-concept ".gz"))
(def name-url (str info-url fname-name ".gz"))
(def save-path "./resources/downloads/")
(def output-path "./resources/stage_0_outputs/medgen.csv")
(def file-path-concept (file-path save-path fname-concept))
(def file-path-name (file-path save-path fname-name))
(def file-path-mapping (file-path save-path fname-mapping))

(defn run
  [_]
  (download-file concept-url save-path fname-concept)
  (download-file name-url save-path fname-name)
  (download-file mapping-url save-path fname-mapping)
  (let [medgen-info (get-results file-path-concept file-path-name)
        medgen-mapping (->> (get-result-mapping file-path-mapping)
                            (map #(set/rename-keys % {:hasDbXref :hasDbXref_1 :dbXref_source :dbXref_source_1})))
        medgen-combined (kg/joiner medgen-info medgen-mapping :id :id kg/left-join)
        medgen-dbXref (map #(select-keys % [:id :label :hasDbXref :dbXref_source :synonym]) medgen-combined)
        medgen-dbXref-1 (->> (map #(select-keys % [:id :label :hasDbXref_1 :dbXref_source_1 :synonym]) medgen-combined)
                             (map #(set/rename-keys % {:hasDbXref_1 :hasDbXref :dbXref_source_1 :dbXref_source})))
        medgen (->> (concat medgen-dbXref medgen-dbXref-1)
                    (map #(assoc % :subClassOf ""))
                    (map #(assoc % :source_id (:id %)))
                    distinct)]
    (kg/write-csv [:id :label :source_id :subClassOf :hasDbXref :dbXref_source :synonym] output-path medgen)))
