(ns sv.gcloud.datastore.util.time
  (:require [clj-time.coerce :as tc]
            [clj-time.format :as tf]
            [clojure.string :as str]))

(defn unparse-zulu-date-format [date]
  (str
   (tf/unparse
    (:date-hour-minute-second-ms tf/formatters)
    (tc/to-date-time date))
   "000000Z"))

(defn parse-zulu-date-format [date-str]
  (tc/to-date
   (tf/parse
    (:date-hour-minute-second-ms tf/formatters)
    (str/replace date-str #"000000Z$" ""))))
