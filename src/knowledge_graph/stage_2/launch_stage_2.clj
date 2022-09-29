(ns knowledge-graph.stage-2.launch-stage-2
  (:require
   [knowledge-graph.stage-2.stage-hasDbXref-rel :as hasDbXref]
   [clojure.tools.logging :as log]))

(defn -main []
  (hasDbXref/run)
  (log/info "finished staging hasDbXref relationships"))
  