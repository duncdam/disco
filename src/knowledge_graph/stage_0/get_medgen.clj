(ns knowledge-graph.stage-0.get-medgen
  (:require
   [clj-http.client :as client]
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
  [file-path-concept file-path-name output-path]
  (with-open [concept-file (io/reader (io/resource file-path-concept))
              name-file (io/reader (io/resource file-path-name))]
    (let [concept-map (->> (slurp concept-file)
                           str/split-lines
                           (map #(str/split % #"\|"))
                           (cons ["CUI" "TS" "STT" "ISPREF" "AUI" "SAUI" "SCUI" "SDUI" "SAB" "TTY" "CODE" "STR" "SUPPRESS"])
                           kg/csv->map
                           (filter #(not= (:CUI %) "#CUI"))
                           (map #(set/rename-keys % {:CUI :id :STT :label-type :CODE :hasDbXref :SAB :source :STR :synonym}))
                           (map #(select-keys % [:id :hasDbXref :source :synonym])))
          name-map (->> (slurp name-file)
                        str/split-lines
                        (map #(str/split % #"\|"))
                        (cons ["CUI" "name" "source" "SUPPRESS"])
                        kg/csv->map
                        (filter #(not= (:CUI %) "#CUI"))
                        (map #(set/rename-keys % {:CUI :id :name :label}))
                        (map #(select-keys % [:id :label])))
          medgen (kg/joiner name-map concept-map :id :id kg/inner-join)]
          (->> (mapv #(select-keys % [:id :label :hasDbXref :source :synonym]) medgen)
               distinct
               (kg/write-csv [:id :label :hasDbXref :source :synonym] output-path)))))

(defn file-path
  [save-path fname]
  (-> (str save-path fname)
      (str/replace #"/resources" "")))

(defn run
  [_]
  (let [url "https://ftp.ncbi.nlm.nih.gov/pub/medgen/"
        fname-concept "MGCONSO.RRF"
        fname-name "NAMES.RRF"
        concept-url (str url fname-concept ".gz")
        name-url (str url fname-name ".gz")
        save-path "./resources/downloads/"
        output-path "./resources/stage_0_outputs/medgen.csv"
        file-path-concept (file-path save-path fname-concept)
        file-path-name (file-path save-path fname-name)]
    (download-file concept-url save-path fname-concept)
    (download-file name-url save-path fname-name)
    (get-results file-path-concept file-path-name output-path)))
