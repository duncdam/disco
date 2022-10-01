(ns knowledge-graph.stage-2.launch-stage-2
  (:require
   [knowledge-graph.stage-2.stage-hasDbXref-rel :as hasDbXref]
   [knowledge-graph.stage-2.stage-altLabel-rel :as altLabel]
   [knowledge-graph.stage-2.stage-prefLabel-rel :as prefLabel]
   [clojure.tools.logging :as log]))

(defn -main []
  (hasDbXref/run)
  (log/info "finished staging hasDbXref relationships")
  (altLabel/run)
  (log/info "finished staging altLabel relationship")
  (prefLabel/run)
  (log/info "finished staging prefLabel relationship"))
  