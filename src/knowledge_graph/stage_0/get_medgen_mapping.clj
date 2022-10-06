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
          medgen-mapping (->> (map #(set/rename-keys % {:#CUI :id :source_id :hasDbXref :source :source}) data-map)
                              (map #(assoc % :hasDbXref (str/upper-case (:hasDbXref %))))
                              (map #(assoc % :hasDbXref (str/replace (:hasDbXref %) "_" ":")))
                              (map #(assoc % :hasDbXref (cond 
                                (not(str/includes? (:hasDbXref %) ":")) (str/join ":" [(:source %) (:hasDbXref %)])
                                :else (:hasDbXref %))))
                              (mapv #(select-keys % [:id :hasDbXref :source])))
          medgen-umls (->> (map #(assoc % :hasDbXref (str/join ":" ["UMLS" (:id %)])) medgen-mapping)
                           (map #(assoc % :source "UMLS"))
                           (mapv #(select-keys % [:id :hasDbXref :source])))
          mapping (->> (concat medgen-mapping  medgen-umls)
                       distinct)]
          (->> (filter #(some? (:id %)) mapping)
               (kg/write-csv [:id :hasDbXref :source] output-path)))))

(defn run
  [_]
  (let [url "https://ftp.ncbi.nlm.nih.gov/pub/medgen/MedGenIDMappings.txt.gz"
        save-path "./resources/medgen/"
        fname "medgen_id_mapping.txt"
        file-path (-> (str save-path fname)
                      (str/replace #"/resources" ""))
        output-path "./resources/stage_0_outputs/medgen_mapping.csv"]
    (download-file url save-path fname)
    (get-results file-path output-path)))

