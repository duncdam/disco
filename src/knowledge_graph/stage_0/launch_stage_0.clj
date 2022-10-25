(ns knowledge-graph.stage-0.launch-stage-0
  (:require
   [knowledge-graph.stage-0.get-icd10 :as icd10]
   [knowledge-graph.stage-0.get-icd9 :as icd9]
   [knowledge-graph.stage-0.get-doid :as dx]
   [knowledge-graph.stage-0.get-mondo :as mx]
   [knowledge-graph.stage-0.get-orphanet :as po]
   [knowledge-graph.stage-0.get-efo :as efo]
   [knowledge-graph.stage-0.get-hpo :as hpo]
   [knowledge-graph.stage-0.get-mesh-des :as mdx]
   [knowledge-graph.stage-0.get-mesh-scr :as msx]
   [knowledge-graph.stage-0.get-snomedct :as snomedct]
   [knowledge-graph.stage-0.get-ncit :as ncit]
   [knowledge-graph.stage-0.get-meddra :as meddra]
   [knowledge-graph.stage-0.get-kegg :as kegg] 
   [knowledge-graph.stage-0.get-medgen-mapping :as mdm]
   [knowledge-graph.stage-0.get-ncit-mapping :as nnm]
   [knowledge-graph.stage-0.get-snomedct-mapping :as si10]
   [knowledge-graph.stage-0.get-icd9-mapping :as icd910]
   [knowledge-graph.stage-0.get-kegg-mapping :as kem]
   [clojure.tools.logging :as log]))

(defn -main []
  (dx/run '_)
  (log/info "finished parsing DOID")
  (po/run '_)
  (log/info "finished parsing Orphanet")
  (efo/run '_)
  (log/info "finished parsing EFO")
  (hpo/run '_)
  (log/info "finished parsing HPO")
  (mx/run '_)
  (log/info "finished parsing MONDO")
  (mdx/run '_)
  (log/info "finished parsing MESH Descriptors")
  (msx/run '_)
  (log/info "finished parsing MESH SCR")
  (icd10/run '_)
  (log/info "finished getting ICD10")
  (icd9/run '_)
  (log/info "finished getting ICD9")
  (snomedct/run '_)
  (log/info "finished getting SNOMEDCT")
  (ncit/run '_)
  (log/info "finish getting NCIT")
  (meddra/run '_)
  (log/info "finish getting MEDDRA")
  (kegg/run '_)
  (log/info "finish getting KEGG")
  (mdm/run '_)
  (log/info "finished MEDGEN mapping")
  (nnm/run '_)
  (log/info "finished NCIT mapping")
  (si10/run '_)
  (log/info "finished SNOMED mapping")
  (icd910/run '_)
  (log/info "finish ICD9 mapping")
  (kem/run '_)
  (log/info "finish KEGG mapping")
  )

