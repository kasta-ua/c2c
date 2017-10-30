(ns c2c.utils
  (:require   [clojure.tools.logging :as log]
              [clojure.string        :as str]
              [clojure.java.io       :as io]
              [qbits.spandex.utils   :as utils]))

(defn drop-prefix [st prefix]
  (subs st (count prefix)))


(defn exists [st]
  (and st (not (str/blank? st))))


(defn merge-specs [parsed-urls]
  (reduce
    (fn [new-map {:keys [username password hosts]}]
      (cond-> new-map
        (and (exists username) (exists password)
             (not (:username new-map)))
        (assoc :username username :password password)

        (and hosts (not-empty hosts))
        (update :hosts concat hosts)))
    {}
    parsed-urls))


(defn make-auth
  [auth]
  (let [v (str/split auth #":")]
    {:user     (first v)
     :password (second v)}))


(defn make-index-type [path]
  (let [v (filter exists (str/split path #"/"))]
    {:index (first v)
     :type  (second v)}))


(defn parse-url [url]
  (let [ju       (io/as-url (str "http://" url)) ;; http only
        protocol (.getProtocol ju)
        host     (.getHost ju)
        path     (.getPath ju)
        port     (.getPort ju)
        auth     (.getUserInfo ju)]
    (cond-> {}
      (exists protocol)   (assoc :protocol protocol)
      (exists host)       (assoc :host host)
      (exists path)       (assoc :path path)
      port                (assoc :port port)
      (exists auth)       (merge (make-auth auth))
      (exists path)       (merge (make-index-type path))
      (and (exists protocol)
           (exists host)) (assoc :hosts [(str protocol "://" host (when port (str ":" port)))]))))


(defn not-valid? [url]
  (cond
    (str/blank? url)                    (str "Blank" url "!")
    (not (str/starts-with? url "http")) (str "Required protocol http/https" url "!")
    :else                               nil))


(defn not-valid-parsed? [m]
  (some false?
    (map exists (vals (select-keys m [:protocol :host :path])))))


(defmacro eval-time
  [expr]
  `(let [start# (. System (nanoTime))
         ret#   ~expr]
     (log/info "Time: " (/ (double (- (. System (nanoTime)) start#)) 1000000000.0) " secs")
     ret#))
