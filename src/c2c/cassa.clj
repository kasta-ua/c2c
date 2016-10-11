(ns c2c.cassa
  (:require [clojure.tools.logging :as log]
            [qbits.alia :as alia]
            [qbits.hayt :as hayt]))

(defn get-conns [from to {:keys [fetch-size]}]
  (let [cluster-from (alia/cluster {:contact-points (:ips from)
                                    :query-options {:fetch-size fetch-size}})
        conn-from    (alia/connect cluster-from (:keyspace from))
        cluster-to   (alia/cluster {:contact-points (:ips to)
                                    :query-options {:fetch-size fetch-size}})
        conn-to      (alia/connect cluster-to (:keyspace to))]
    {:cluster-from cluster-from
     :conn-from    conn-from
     :table-from   (:table from)
     :cluster-to   cluster-to
     :conn-to      conn-to
     :table-to     (:table to)}))


(defn copy [from to {:keys [report fetch-size insert-size]}]
  (let [conns (get-conns from to {:fetch-size fetch-size})
        total (atom 0)]
    (log/info "Start!")

    (doseq [rows (partition insert-size insert-size nil
                   (alia/execute (:conn-from conns)
                     (hayt/->raw {:select (:table-from conns)
                                  :columns [:*]})))]
      (alia/execute (:conn-to conns)
        {:logged true
         :batch (map (fn [row] {:insert (:table-to conns)
                                :values row})
                  rows)})
      (swap! total inc)
      (when (zero? (mod @total report))
        (log/info (* @total insert-size) "rows processed")))

    (log/info "End!")))
