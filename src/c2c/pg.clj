(ns c2c.pg
  (:import [org.postgresql.util PGobject]
           [java.util.Date]
           [java.sql.Date]
           [java.sql.Timestamp])
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql-postgres.format]

            [c2c.copy :as copy]))


(extend-protocol jdbc/ISQLValue
  java.util.Date
  (sql-value [v]
    (java.sql.Timestamp. (.getTime v))#_
    (java.sql.Date/valueOf (.toInstant v))))


(defrecord PG [;; are set in `connect`
               conn table fetch-size]
  copy/Conn
  (connect [this {:keys [uri table fetch-size]}]
    (let [conn (jdbc/get-connection {:connection-uri (str "jdbc:" uri)})]
      (assoc this :conn conn :table table :fetch-size fetch-size)))

  copy/From
  (read [this]
    (let [[query & args] (sql/format {:select [:*]
                                      :from [(keyword table)]})]
      (with-open [st (jdbc/prepare-statement conn query
                       {:result-type :forward-only
                        :concurrency :read-only
                        :fetch-size fetch-size})]
        (jdbc/query conn [st args]))))

  copy/To
  (write [this rows]
    (jdbc/execute! {:connection conn}
      (sql/format
        {:insert-into (keyword table)
         :values rows
         :upsert {:on-conflict [:id]
                  :do-update-set (keys (dissoc (first rows) :id))}}))))


(defn get-conn [{:keys [uri table fetch-size]}]
  (let [pg (map->PG {})]
    (copy/connect pg
      {:uri uri :table table :fetch-size fetch-size})))
