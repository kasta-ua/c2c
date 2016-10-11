(ns c2c.main
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [unilog.config :as unilog]
            [qbits.alia :as alia]
            [qbits.hayt :as hayt]))

(unilog/start-logging!
  {:level "info"
   :console true})


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


(defn c2c [from to {:keys [report fetch-size insert-size]}]
  (let [conns (get-conns from to {:fetch-size fetch-size})
        total (atom 0)]
    (log/info "Start!")

    (doseq [rows (partition insert-size
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


(defn fail [code message]
  (println message)
  (System/exit code))


(def IP-RE #"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$")


(def cli-options
  [["-h" "--help" "Show help"]

   [nil "--src-ips IP,IP" "Cassandra ips to copy from (required)"
    :parse-fn #(str/split % #",")
    :validate [(fn [ips] (every? #(re-matches IP-RE %) ips))
               "enter valid ip addresses"]]

   [nil "--dst-ips IP,IP" "Cassandra ips to copy to (default: src-ips)"
    :parse-fn #(str/split % #",")
    :validate [(fn [ips] (every? #(re-matches IP-RE %) ips))
               "enter valid ip addresses"]]

   [nil "--from KEYSPACE.TABLE" "Where to copy from (required)"
    :parse-fn #(str/split % #"\.")
    :validate [#(= 2 (count %)) "invalid format for KEYSPACE.TABLE"]]

   [nil "--to KEYSPACE.TABLE" "Where to copy to (default: from)"
    :parse-fn #(str/split % #"\.")
    :validate [#(= 2 (count %)) "invalid format for KEYSPACE.TABLE"]]

   [nil "--fetch-size N" "Fetch data from Cassandra by batches of N entries"
    :parse-fn #(Integer. %)
    :default 10000
    :validate [pos? "should be positive integer"]]

   [nil "--insert-size N" "Insert data to Cassandra by batches of N entries"
    :parse-fn #(Integer. %)
    :default 100
    :validate [pos? "should be positive integer"]]

   [nil "--report N" "Report progress every N inserts"
    :parse-fn #(Integer. %)
    :default 10
    :validate [pos? "should be positive integer"]]])


(defn -main
  "Copy from one Cassandra DB to another"
  [& args]
  (let [{:keys [options arguments summary errors]}
        (cli/parse-opts args cli-options)]
    (cond
      errors
      (fail 1 (str "Errors: " (str/join "\n\t" errors)))

      (:help options)
      (fail 0 (str "Copy from one Cassandra DB to another.\n\nUsage:\n" summary))

      (nil? (:src-ips options))
      (fail 1 "Error: source ips are required (run with --help to see help)")

      (nil? (:from options))
      (fail 1 "Error: please specify `from` (you want to copy something, do you?)")

      :else
      (let [{:keys [src-ips from dst-ips to]} options]
        (c2c
          {:ips      src-ips
           :keyspace (first from)
           :table    (second from)}
          {:ips      (or dst-ips src-ips)
           :keyspace (first (or to from))
           :table    (second (or to from))}
          (select-keys options
            [:report :fetch-size :insert-size]))
        (System/exit 0)))))

