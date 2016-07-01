(ns sv.gcloud.datastore.entity.parse
  (:require [sv.gcloud.datastore.util.time :as t]))

(defmulti parse-value ffirst)

(defmethod parse-value :nullValue [value]
  nil)

(defmethod parse-value :booleanValue [value]
  (:booleanValue value))

(defmethod parse-value :integerValue [value]
  (Long/valueOf (:integerValue value)))

(defmethod parse-value :doubleValue [value]
  (Double/valueOf (:doubleValue value)))

(defmethod parse-value :timestampValue [value]
  (t/unparse-zulu-date-format (:timestampValue value)))

(defmethod parse-value :stringValue [value]
  (:stringValue value))

(defmethod parse-value :arrayValue [value]
  (map parse-value (:arrayValue value)))

(defn parse-properties [properties-response]
  (into
   {}
   (map
    (fn [[k value-response]]
      [(keyword k) (parse-value value-response)])
    properties-response)))

(defn parse-key [entity-response]
  (let [key (:key (:entity entity-response))
        path (last (:path key))]
    (assert key)
    (merge
     {:ds/kind (:kind path)}
     (when-let [namespace-id (get-in key [:partitionId :namespaceId])]
       {:ds.namespace/id namespace-id})
     (when-let [id (:id path)]
       {:ds/id (Long/parseLong id)})
     (when-let [name (:name path)]
       {:ds/name name}))))

(defn parse-entity [entity-response]
  (merge
   (parse-properties
    (:properties (:entity entity-response)))
   (parse-key entity-response)))
