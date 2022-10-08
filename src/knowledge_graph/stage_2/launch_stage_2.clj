(ns knowledge-graph.stage-2.launch-stage-2
  (:require
   [knowledge-graph.stage-2.stage-hasDbXref-rel :as hasDbXref]
   [knowledge-graph.stage-2.stage-prefLabel-rel :as prefLabel]
   [clojure.tools.logging :as log]))

(defn -main []
  (hasDbXref/run)
  (log/info "finished staging hasDbXref relationships")
  (prefLabel/run)
  (log/info "finished staging prefLabel relationship"))
  