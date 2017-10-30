(ns c2c.cassa
  (:import [java.net URL])
  (:require [clojure.tools.logging :as log]
            [qbits.alia :as alia]
            [qbits.hayt :as hayt]

            [c2c.copy :as copy]
            [clojure.string :as str]))


(defrecord Cassa [;; are set in `connect`
                  cluster conn table]
  copy/Conn
  (connect [this opts]
    (let [cluster (alia/cluster {:contact-points (:ips opts)
                                 :query-options  {:fetch-size (:fetch-size opts)}})
          conn    (alia/connect cluster (:keyspace opts))]
      (println cluster conn)
      (assoc this :cluster cluster :conn conn :table (:table opts))))

  copy/From
  (read [this]
    (alia/execute conn
      (hayt/->raw {:select  table
                   :columns [:*]})))

  copy/To
  (write [this rows]
    (alia/execute conn
      {:logged true
       :batch  (->> rows
                    (map (fn [row] {:insert table
                                    :values row})))}))
  copy/Clean
  (clean [this] (identity)))


(def RE #"cassandra://([^/]+)/([^/]+)")


(defn get-conn [{:keys [uri table fetch-size]}]
  (let [cassa (map->Cassa {})
        [_ ips keyspace] (re-find RE uri)]
    (copy/connect cassa
      {:ips (str/split ips #",")
       :fetch-size fetch-size
       :keyspace keyspace
       :table table})))
