(ns knowledge-graph.stage-0.launch-stage-0
  (:require
   [knowledge-graph.stage-0.parse-doid :as dx]
   [knowledge-graph.stage-0.get-medgen-mapping :as mdm]
   [knowledge-graph.stage-0.get-ncit-meddra-mapping :as nmm]
   [knowledge-graph.stage-0.get-ncit-neoplasm-mapping :as nnm]
   [knowledge-graph.stage-0.get-snomedct-icd10-mapping :as si10]
   [knowledge-graph.stage-0.get-icd10 :as icd10]
   [knowledge-graph.stage-0.get-icd9 :as icd9]
   [knowledge-graph.stage-0.get-icd10-icd9-mapping :as icd910]
   [knowledge-graph.stage-0.parse-mondo :as mx]
   [knowledge-graph.stage-0.parse-orphanet :as po]
   [knowledge-graph.stage-0.parse-efo :as efo]
   [knowledge-graph.stage-0.parse-hpo :as hpo]
   [knowledge-graph.stage-0.parse-mesh-descriptor :as mdx]
   [knowledge-graph.stage-0.parse-mesh-scr :as msx]
   [knowledge-graph.stage-0.get-umls :as umls]
   [clojure.tools.logging :as log]))

(defn -main []
  (dx/run)
  (log/info "finished parsing DOID")
  (mdm/run)
  (log/info "finished medgen mapping")
  (nmm/run)
  (log/info "finished NCIT-MEDDRA mapping")
  (nnm/run)
  (log/info "finished NCIT-NEOPLASM mapping")
  (si10/run)
  (log/info "finished SNOMED-ICD10 mapping")
  (icd10/run)
  (log/info "finished getting ICD10")
  (icd9/run)
  (log/info "finished getting ICD9")
  (icd910/run)
  (log/info "finish IC10-ICD9 mapping")
  (po/run)
  (log/info "finished parsing Orphanet")
  (efo/run)
  (log/info "finished parsing EFO")
  (hpo/run)
  (log/info "finished parsing HPO")
  (mx/run)
  (log/info "finished parsing MONDO")
  (mdx/run)
  (log/info "finished parsing MESH Descriptors")
  (msx/run)
  (log/info "finished parsing MESH SCR")
  (umls/run)
  (log/info "finished getting UMLS"))

