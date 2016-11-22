(ns sv.gcloud.datastore.entity.parse
  (:require [sv.gcloud.datastore.util.time :as t]))

(declare parse-value)

(def parse-fns
  {:stringValue :stringValue
   :integerValue #(Long/valueOf (:integerValue %))
   :booleanValue :booleanValue
   :nullValue (constantly nil)
   :doubleValue #(Double/valueOf (:doubleValue %))
   :timestampValue #(t/unparse-zulu-date-format (:timestampValue %))
   :arrayValue #(map parse-value (:arrayValue %))})

(defn parse-value [value]
  (if-let [parse-fn (some
                     (fn [[k f]]
                       (when (contains? value k)
                         f))
                     parse-fns)]
    (parse-fn value)
    (throw (ex-info "can not parse value" {:value value}))))

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
