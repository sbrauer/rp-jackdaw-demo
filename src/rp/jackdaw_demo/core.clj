(ns rp.jackdaw-demo.core
  (:require [rp.jackdaw.topic-registry :as reg]
            [rp.jackdaw.processor :as proc]
            [jackdaw.streams :as streams]
            [com.stuartsierra.component :as component]))

(def bootstrap-servers "localhost:9092")

(def app-config
  {"application.id" "demo-processor"
   "bootstrap.servers" bootstrap-servers})

(def topic-metadata
  {:users {:topic-name "users"
           :key-serde {:serde-keyword :jackdaw.serdes.edn/serde}
           :value-serde {:serde-keyword :jackdaw.serdes.edn/serde}
           :partition-count 1
           :replication-factor 1}
   :events {:topic-name "user.events"
            :key-serde {:serde-keyword :jackdaw.serdes.edn/serde}
            :value-serde {:serde-keyword :jackdaw.serdes.edn/serde}
            :partition-count 1
            :replication-factor 1}})

(defn topology-builder
  [{:keys [topic-configs] :as component}]
  (fn [builder]
    (let [users (streams/ktable builder (:users topic-configs))
          events (streams/kstream builder (:events topic-configs))]
      (-> (streams/left-join events users (fn [event user]
                                            (assoc event :user user)))
          (streams/print!)))
    builder))

(def sysmap
  (component/system-map
   :topic-registry (reg/map->TopicRegistry
                    {:topic-metadata topic-metadata})
   :processor (component/using
               (proc/map->Processor
                {:app-config app-config
                 :topology-builder-fn topology-builder})
               [:topic-registry])))

(defn -main
  [& args]
  (component/start sysmap))

(comment
  (require '[rp.jackdaw.user :refer :all])

  (delete-topics! (admin-config bootstrap-servers) (vals topic-metadata))
  (create-topics! (admin-config bootstrap-servers) (vals topic-metadata))

  (def sys (component/start sysmap))

  (produce! (producer-config bootstrap-servers)
            (get-in sys [:topic-registry :topic-configs :users])
            1 {:firstname "Sam" :lastname "Brauer"})

  (produce! (producer-config bootstrap-servers)
            (get-in sys [:topic-registry :topic-configs :users])
            2 {:firstname "Eric" :lastname "Caspary"})

  (produce! (producer-config bootstrap-servers)
            (get-in sys [:topic-registry :topic-configs :events])
            1 {:event "foo"})

  (produce! (producer-config bootstrap-servers)
            (get-in sys [:topic-registry :topic-configs :users])
            1 {:firstname "Sammy" :lastname "Brauer"})

  (produce! (producer-config bootstrap-servers)
            (get-in sys [:topic-registry :topic-configs :events])
            1 {:event "bar"})

  (produce! (producer-config bootstrap-servers)
            (get-in sys [:topic-registry :topic-configs :events])
            2 {:event "beep!"})

  (def sys (component/stop sysmap))
  )
