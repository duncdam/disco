(ns knowledge-graph.stage-1.launch-stage-1
  (:require
   [knowledge-graph.stage-1.stage-disease-nodes :as disease-nodes]
   [knowledge-graph.stage-1.stage-synonym-nodes :as synonym-nodes]
   [taoensso.timbre :as log]))

(defn -main []
  (disease-nodes/run '_)
  (log/info "finish staging all DISEASE nodes")
  (synonym-nodes/run '_)
  (log/info "finish staging all SYNONYM nodes"))