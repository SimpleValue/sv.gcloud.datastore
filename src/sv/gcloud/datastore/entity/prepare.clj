(ns sv.gcloud.datastore.entity.prepare
  (:require [sv.gcloud.datastore.util.time :as t]
            [clojure.string :as str]))

(defprotocol DatastoreValue
  (prepare-value [this]))

(extend-protocol DatastoreValue
  nil
  (prepare-value [this]
    {:nullValue this})
  Boolean
  (prepare-value [this]
    {:booleanValue this})
  Integer
  (prepare-value [this]
    {:integerValue this})
  Long
  (prepare-value [this]
    {:integerValue this})
  Float
  (prepare-value [this]
    {:doubleValue this})
  Double
  (prepare-value [this]
    {:doubleValue this})
  java.util.Date
  (prepare-value [this]
    {:timestampValue (t/unparse-zulu-date-format this)})
  String
  (prepare-value [this]
    {:stringValue this})
  java.util.List
  (prepare-value [this]
    {:arrayValue (map prepare-value this)}))

(defn prepare-properties [entity]
  (into
   {}
   (keep
    (fn [[k v]]
      (when-not (str/starts-with? (str k) ":ds")
        [(str/replace (str k) #"^:" "")
         (assoc
          (prepare-value v)
          ;; Datastore indexes every property by default, this could
          ;; lead to problems, for example if the value of a string
          ;; property is too long for indexing. This library includes
          ;; no property in the index by default. If you like to index
          ;; a property include its key into the :ds/indexes entry of
          ;; the entity map:
          ;;
          ;; {:ds/kind "entity-key"
          ;;  :ds/name "123"
          ;;  :ds/indexes #{:name} ;; :name will be indexed
          ;;  :name "my entity"}
          :excludeFromIndexes
          (not (contains? (:ds/indexes entity) k)))]))
    entity)))

(defn prepare-key [entity]
  {:partitionId
   (when-let [namespace-id (:ds.namespace/id entity)]
     {:namespaceId namespace-id})
   :path
   [(merge
     {:kind (:ds/kind entity)}
     (when-let [id (:ds/id entity)]
       {:id id})
     (when-let [name (:ds/name entity)]
       {:name name}))]})

(defn prepare-entity [entity]
  {:key
   (prepare-key entity)
   :properties
   (prepare-properties entity)})
