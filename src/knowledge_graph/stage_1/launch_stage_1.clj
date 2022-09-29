(ns knowledge-graph.stage-1.launch-stage-1
  (:require
   [knowledge-graph.stage-1.stage-medgen-nodes :as medgen]
   [knowledge-graph.stage-1.stage-icd10-nodes :as icd10]
   [knowledge-graph.stage-1.stage-icd9-nodes :as icd9]
   [knowledge-graph.stage-1.stage-ncit-nodes :as ncit]
   [knowledge-graph.stage-1.stage-mondo-nodes :as mondo]
   [knowledge-graph.stage-1.stage-doid-nodes :as doid]
   [knowledge-graph.stage-1.stage-efo-nodes :as efo]
   [knowledge-graph.stage-1.stage-snomedct-nodes :as snomedct]
   [knowledge-graph.stage-1.stage-hpo-nodes :as hpo]
   [knowledge-graph.stage-1.stage-mesh-nodes :as mesh]
   [knowledge-graph.stage-1.stage-orphanet-nodes :as orpha]
   [knowledge-graph.stage-1.stage-umls-nodes :as umls]
   [knowledge-graph.stage-1.stage-meddra-nodes :as meddra]
   [knowledge-graph.stage-1.stage-disease-nodes :as disease-nodes]
   [knowledge-graph.stage-1.stage-synonym-nodes :as synonym-nodes]
   [clojure.tools.logging :as log]))

(defn -main []
  (medgen/run)
  (log/info "finished staging MEDGEN nodes")
  (icd10/run)
  (log/info "finished staging ICD10 nodes")
  (icd9/run)
  (log/info "finished staging ICD9 nodes")
  (ncit/run)
  (log/info "finish staging NCIT nodes")
  (meddra/run)
  (log/info "finish staging MEDDRA nodes")
  (mondo/run)
  (log/info "finish staging MONDO nodes")
  (doid/run)
  (log/info "finish staging DOID nodes")
  (efo/run)
  (log/info "finish staging EFO nodes")
  (snomedct/run)
  (log/info "finish staging SNOMEDCT nodes")
  (hpo/run)
  (log/info "finish staging HPO nodes")
  (mesh/run)
  (log/info "finish staging MESH nodes")
  (orpha/run)
  (log/info "finish staging ORPHANET nodes")
  (umls/run)
  (log/info "finish staging UMLS nodes")
  (disease-nodes/run)
  (log/info "finish staging all DISEASE nodes")
  (synonym-nodes/run)
  (log/info "finish staging all SYNONYM nodes"))


