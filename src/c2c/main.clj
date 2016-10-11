(ns c2c.main
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [unilog.config :as unilog]

            [c2c.cassa :as cassa]))


(unilog/start-logging!
  {:level "info"
   :console true})


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
        (cassa/copy
          {:ips      src-ips
           :keyspace (first from)
           :table    (second from)}
          {:ips      (or dst-ips src-ips)
           :keyspace (first (or to from))
           :table    (second (or to from))}
          (select-keys options
            [:report :fetch-size :insert-size]))
        (System/exit 0)))))

