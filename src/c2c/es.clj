(ns c2c.es
  (:require [clojure.tools.logging :as log]
            [clojure.string        :as str]
            [clojure.core.async    :as async]
            [qbits.spandex         :as esx]
            [qbits.spandex.utils   :as esu]
            [c2c.copy              :as copy]
            [c2c.utils             :as utils]))

(def skip-es-status #{400 404 409 500 502})
(defn exception-handler [ex]
  (let [x      (esx/decode-exception ex)
        res    (ex-data x)
        status (:status res)]
    (cond
      (and (instance? Exception x)
           (contains? skip-es-status status))
      (do
        (log/info "exception-handler" (:body res))
        res)

      (instance? Exception x)
      (throw x)

      :else x)))


(def used-scrolls (ref #{}))
(defn alter-scrolls! [page]
  (when-let [scroll-id (-> page :body :_scroll_id)]
    (dosync (alter used-scrolls conj scroll-id))))


(defn clean-scrolls [conn]
  (let [xs (vec @used-scrolls)]
    (when (and xs (not-empty xs))
      (esx/request conn
        {:url               "/_search/scroll"
         :method            :delete
         :exception-handler exception-handler
         :body              {:scroll_id xs}}))))


(defn create-ndjson [docs index type]
  (reduce (fn [acc doc]
            (-> acc
                (conj {:create {:_id    (:_id doc)
                                :_index (or index (:_index doc))
                                :_type  (or type  (:_type doc))}})
                (conj (:_source doc))))
    [] docs))


(defn scroll! [conn uri]
  (let [ch (esx/scroll-chan conn
             {:url               (-> uri (str/split #"\/") (conj :_search))
              :exception-handler exception-handler
              :body              {:query {:match_all {}}}})]
    ch))


(defn swap-result! [response]
  (let [items         (some-> response :body :items seq)
        error-count   (count (filterv #(-> % :create :error) items))
        created-count (-> (count items) (- error-count))]
    (swap! copy/result update :processed (fn [x] (+ x (count items))))
    (swap! copy/result update :created (fn [x] (+ x created-count)))
    (swap! copy/result update :error (fn [x] (+ x error-count)))))


(defn bulk! [conn uri es-respons]
  (let [index (-> uri (str/split #"\/") first)
        type  (-> uri (str/split #"\/") second)
        _     (mapv alter-scrolls! es-respons)
        docs' (mapv #(some-> % :body :hits :hits seq) es-respons)
        docs  (mapcat #(create-ndjson % index type) docs')]
    (swap-result!
      (esx/request conn
        {:url               "/_bulk"
         :method            :put
         :headers           {"Content-Type" "application/x-ndjson"}
         :exception-handler exception-handler
         :body              (esx/chunks->body docs)}))))


(defrecord ES [conn]
  copy/Conn
  (connect [this {:keys [spec uri]}]
    (assoc this :conn (esx/client spec) :uri uri))

  copy/From
  (read [this]
    (esu/chan->seq (scroll! conn (:uri this))))

  copy/To
  (write [this es-respons]
    (bulk! conn (:uri this) es-respons))

  copy/Clean
  (clean [this]
    (clean-scrolls conn)))


(defn get-conn [{:keys [uri table]}]
  (let [uris            (-> uri (utils/drop-prefix "es://") (str/split #","))
        {:keys [username
                password
                hosts]} (utils/merge-specs (mapv utils/parse-url uris))
        es              (map->ES {})
        spec            (cond-> {:hosts hosts}
                          (and username password)
                          (assoc :http-client {:basic-auth
                                               {:user     username
                                                :password password}}))]
    (copy/connect es
      {:spec spec
       :uri  table})))
