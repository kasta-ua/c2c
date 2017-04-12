(ns c2c.copy
  (:refer-clojure :exclude [read])
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.string :as str]))


(defprotocol Conn
  (connect [this opts]))


(defprotocol From
  (read [this]))


(defprotocol To
  (write [this rows]))


(defn gen-mapper [mapping]
  (let [kmap (->> (str/split mapping #",")
                  (map #(str/split % #":"))
                  (reduce (fn [m [k v]] (assoc m (keyword k) (keyword v))) {}))]
    (fn [row]
      (set/rename-keys row kmap))))


(defn copy [source dest {:keys [report insert-size mapping]}]
  (let [total (atom 0)
        mapper (if mapping (gen-mapper mapping) identity)]
    (log/info "Start!")

    (doseq [rows (partition insert-size insert-size nil
                   (read source))]
      (write dest (map mapper rows))
      (swap! total inc)
      (when (zero? (mod @total report))
        (log/info (* @total insert-size) "rows processed")))

    (log/info "End!")))
