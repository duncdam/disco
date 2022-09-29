(ns knowledge-graph.stage-0.get-medgen-mapping
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
  [url save-path fname output-path]
  (download-file url save-path fname)
  (def file-path (-> (str save-path fname)
                     (str/replace #"/resources", "")))
  (with-open [file (io/reader (io/resource file-path))]
    (->> (csv/read-csv file :separator \|)
         vec
         kg/csv->map
         (map #(set/rename-keys % {:#CUI      :medgen_id
                                   :pref_name :medgen_name
                                   :source_id :source_id
                                   :source    :source}))
         (filter #(some? (:medgen_id %)))
         (kg/write-csv [:medgen_id :medgen_name :source_id :source] output-path))))

(defn run
  []
  (let [url "https://ftp.ncbi.nlm.nih.gov/pub/medgen/MedGenIDMappings.txt.gz"
        save-path "./resources/medgen/"
        fname "medgen_id_mapping.txt"
        output-path "./resources/stage_0_outputs/medgen_id_mapping.csv"]
    (get-results url save-path fname output-path)))

