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
    (let [concept-map (->> (slurp concept-file)
                           str/split-lines
                           (map #(str/split % #"\|"))
                           (cons ["CUI" "TS" "STT" "ISPREF" "AUI" "SAUI" "SCUI" "SDUI" "SAB" "TTY" "CODE" "STR" "SUPPRESS"])
                           kg/csv->map
                           (filter #(not= (:CUI %) "#CUI"))
                           (map #(set/rename-keys % {:CUI :id :STT :label-type :CODE :hasDbXref :SAB :dbXref_source :STR :synonym}))
                           (map #(assoc % :hasDbXref (kg/correct-source-id (:hasDbXref %))))
                           (map #(assoc % :dbXref_source (kg/create-source (:dbXref_source %) (:dbXref_source %))))
                           (map #(select-keys % [:id :hasDbXref :dbXref_source :synonym])))
          name-map (->> (slurp name-file)
                        str/split-lines
                        (map #(str/split % #"\|"))
                        (cons ["CUI" "name" "source" "SUPPRESS"])
                        kg/csv->map
                        (filter #(not= (:CUI %) "#CUI"))
                        (map #(set/rename-keys % {:CUI :id :name :label}))
                        (map #(select-keys % [:id :label])))
          medgen (kg/joiner name-map concept-map :id :id kg/inner-join)]
          (->> (mapv #(select-keys % [:id :label :hasDbXref :dbXref_source :synonym]) medgen)
               distinct))))

(defn get-result-mapping
  [file-path]
  (with-open [file (io/reader (io/resource file-path))]
    (let [data-map(->> (csv/read-csv file :separator \|)
                        kg/csv->map)
          medgen-mapping (->> (map #(set/rename-keys % {:#CUI :id :source_id :hasDbXref :source :dbXref_source}) data-map)
                              (map #(assoc % :dbXref_source (str/upper-case (:dbXref_source %))))
                              (map #(assoc % :hasDbXref (str/upper-case (:hasDbXref %))))
                              (map #(assoc % :hasDbXref (kg/correct-source-id (:hasDbXref %))))
                              (mapv #(select-keys % [:id :hasDbXref :dbXref_source])))
          medgen-umls (->> (map #(assoc % :hasDbXref (str/join ":" ["UMLS" (:id %)])) medgen-mapping)
                            (map #(assoc % :dbXref_source "UMLS"))
                            (mapv #(select-keys % [:id :hasDbXref :dbXref_source])))
          mapping (->> (concat medgen-mapping  medgen-umls)
                        distinct)]
          (->> (filter #(some? (:id %)) mapping)
               (map #(set/rename-keys % {:hasDbXref :hasDbXref_1 :dbXref_source :dbXref_source_1})) 
               (mapv #(select-keys % [:id :hasDbXref_1 :dbXref_source_1]))
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
        medgen-mapping (get-result-mapping file-path-mapping)
        medgen-combined (kg/joiner medgen-info medgen-mapping :id :id kg/left-join)
        medgen-dbXref (map #(select-keys % [:id :label :hasDbXref :dbXref_source :synonym]) medgen-combined)
        medgen-dbXref-1 (->> (map #(set/rename-keys % {:hasDbXref_1 :hasDbXref :dbXref_source_1 :dbXref_source :hasDbXref :_ :dbXref_source :_}) medgen-combined)
                             (map #(select-keys % [:id :label :hasDbXref :dbXref_source :synonym])))
        medgen (->> (concat medgen-dbXref medgen-dbXref-1)
                    distinct)]
    (->>(map #(assoc % :id (str/join "_" ["MEDGEN" (:id %)])) medgen)
        (kg/write-csv [:id :label :hasDbXref :dbXref_source :synonym] output-path))))
