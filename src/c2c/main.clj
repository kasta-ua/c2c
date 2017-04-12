(ns c2c.main
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [unilog.config :as unilog]

            [c2c.cassa :as cassa]
            [c2c.pg :as pg]
            [c2c.copy :as copy]))


(unilog/start-logging!
  {:level "info"
   :console true})


(defn fail [code message]
  (println message)
  (System/exit code))


(def IP-RE #"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$")


(def cli-options
  [["-h" "--help" "Show help"]

   [nil "--src-conn [URI]" "Source DB URI, like cassandra://ip1,ip2/keyspace"]
   [nil "--dst-conn [URI]" "Destination DB URI, like postgresql://user:password@ip1,ip2/dbname"]

   [nil "--from TABLE" "Where to copy from (required)"]

   [nil "--to TABLE" "Where to copy to (default: from)"]

   [nil "--mapping MAP" "Rename columns, format: 'a:b,c:d'"]

   [nil "--fetch-size N" "Fetch data by batches of N entries"
    :parse-fn #(Integer. %)
    :default 10000
    :validate [pos? "should be positive integer"]]

   [nil "--insert-size N" "Insert data by batches of N entries"
    :parse-fn #(Integer. %)
    :default 100
    :validate [pos? "should be positive integer"]]

   [nil "--report N" "Report progress every N inserts"
    :parse-fn #(Integer. %)
    :default 10
    :validate [pos? "should be positive integer"]]])


(defn get-conn [{:keys [uri table fetch-size] :as opts}]
  (cond
    (.startsWith uri "cassandra")
    (cassa/get-conn opts)

    :else
    (pg/get-conn opts)))


(defn -main
  "Copy from one Cassandra DB to another"
  [& args]
  (let [{:keys [options arguments summary errors]}
        (cli/parse-opts args cli-options)]
    (cond
      errors
      (fail 1 (str "Errors: " (str/join "\n\t" errors)))

      (:help options)
      (fail 0 (str "Copy from one DB (Cassandra, Postgres) to another.\n\nUsage:\n" summary))

      (nil? (:src-conn options))
      (fail 1 "Error: source connection url is required (run with --help to see help)")

      (nil? (:dst-conn options))
      (fail 1 "Error: destination connection url is required (run with --help to see help)")

      (nil? (:from options))
      (fail 1 "Error: please specify `from` (you want to copy something, do you?)")

      :else
      (let [{:keys [src-conn dst-conn from to fetch-size]} options
            source (get-conn {:uri src-conn
                              :table from
                              :fetch-size fetch-size})
            dest   (get-conn {:uri dst-conn
                              :table (or to from)
                              :fetch-size fetch-size})]
        (copy/copy source dest
          (select-keys options [:report :insert-size :mapping]))
        (System/exit 0)))))

