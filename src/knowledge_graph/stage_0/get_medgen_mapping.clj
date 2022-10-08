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
  [file-path output-path]
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
               (kg/write-csv [:id :hasDbXref :dbXref_source] output-path)))))

(defn run
  [_]
  (let [url "https://ftp.ncbi.nlm.nih.gov/pub/medgen/MedGenIDMappings.txt.gz"
        save-path "./resources/downloads/"
        fname "medgen_id_mapping.txt"
        file-path (-> (str save-path fname)
                      (str/replace #"/resources" ""))
        output-path "./resources/stage_0_outputs/medgen_mapping.csv"]
    (download-file url save-path fname)
    (get-results file-path output-path)))

