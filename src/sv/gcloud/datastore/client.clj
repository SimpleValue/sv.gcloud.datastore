(ns sv.gcloud.datastore.client
  (:require [sv.gcloud.datastore.entity.prepare :as prepare]
            [sv.gcloud.datastore.entity.parse :as parse]
            [clojure.string :as str]))

(defn wrap-insert-project-id [client config]
  (fn [request]
    (client
     (update
      request
      :url
      (fn [url]
        (when url
          (str/replace
           url
           "{projectId}"
           (:projectId config))))))))

(defn wrap-client [client config]
  (-> client
      (wrap-insert-project-id config)))

(defn create-client [create-client-fn config]
  (-> (create-client-fn
       {:scopes (:scopes config ["https://www.googleapis.com/auth/datastore"])})
      (wrap-client config)))

(defn start-transaction-request [params]
  {:request-method :post
   :url "https://datastore.googleapis.com/v1/projects/{projectId}:beginTransaction"
   :as :json})

(defn start-transaction [client params]
  (-> (start-transaction-request params)
      (client)
      (get-in [:body :transaction])))

(defn lookup-request [params]
  {:request-method :post
   :url "https://datastore.googleapis.com/v1/projects/{projectId}:lookup"
   :form-params
   {:readOptions
    (if-let [transaction (:transaction params)]
      {:transaction transaction}
      {:readConsistency (:readConsistency params "STRONG")})
    :keys (:keys params)}
   :content-type :json
   :as :json})

(defn lookup-entity-request [entity]
  (let [key (prepare/prepare-key entity)]
    (lookup-request
     {:keys [key]})))

(defn lookup-entity [client entity-key]
  (let [response (client
                  (lookup-entity-request
                   entity-key))]
    (when-let [entity (get-in response [:body :found 0])]
      (parse/parse-entity entity))))

(defn commit-request [params]
  {:request-method :post
   :url "https://datastore.googleapis.com/v1/projects/{projectId}:commit"
   :form-params
   (merge
    {:mode (:mode params)
     :mutations (:mutations params)}
    (when-let [transaction (:transaction params)]
      {:transaction transaction}))
   :content-type :json
   :as :json})

(defn swap-entity [client params f]
  (let [key (prepare/prepare-key params)
        response (client
                  (lookup-entity-request params))
        ;; TODO: handle :deferred results
        commit (fn [mutations]
                 (client
                  (commit-request
                   {:mode "NON_TRANSACTIONAL"
                    :mutations mutations})))]
    (if-let [entity (get-in response [:body :found 0])]
      (let [updated-entity (f (parse/parse-entity entity))
            mutations [{:update
                        (prepare/prepare-entity updated-entity)}]]
        (commit mutations)
        updated-entity)
      (when-let [entity (get-in response [:body :missing 0])]
        (let [created-entity (f (parse/parse-entity entity))
              mutations [{:insert
                          (prepare/prepare-entity created-entity)}]]
          (commit mutations)
          created-entity)))))

(defn upsert-entity-request [params]
  (commit-request
   {:mode "NON_TRANSACTIONAL"
    :mutations {:upsert (prepare/prepare-entity (:entity params))}}))

(defn in-transaction [client params f]
  (let [tx-id (start-transaction client {})
        lookup-request (lookup-request
                        (merge
                         (select-keys params [:keys])
                         {:transaction tx-id}))
        lookup-response (client lookup-request)
        lookup-result (:body lookup-response)
        mutations (f lookup-result)
        commit-request (commit-request
                        {:mode "TRANSACTIONAL"
                         :mutations mutations
                         :transaction tx-id})]
    {:commit-result (:body (client commit-request))
     :mutations mutations}))

(defn transact-entities [client params]
  (let [entity-keys (keys (:operations params))]
    (let [result (in-transaction
                  client
                  {:keys [(map prepare/prepare-key entity-keys)]}
                  (fn [lookup-result]
                    (let [results (into
                                   {}
                                   (map
                                    (fn [entity]
                                      [(parse/parse-key entity)
                                       (parse/parse-entity entity)])
                                    (:found lookup-result)))]
                      (map
                       (fn [[key f]]
                         (let [entity (or (get results key)
                                          key)]
                           [{:upsert (prepare/prepare-entity
                                      (f entity))}]))
                       (:operations params)))))]
      result)))
