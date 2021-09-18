(ns xtdb.tx.subscribe
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.io :as xio])
  (:import xtdb.api.ICursor
           java.time.Duration
           [java.util.concurrent CompletableFuture]
           java.util.function.BiConsumer))

(def ^java.util.concurrent.ThreadFactory subscription-thread-factory
  (xio/thread-factory "xtdb-tx-subscription"))

(defn ^CompletableFuture completable-thread [f]
  (let [fut (CompletableFuture.)
        thread (doto (.newThread subscription-thread-factory
                                 (fn []
                                   (try
                                     (f fut)
                                     (.complete fut nil)
                                     (catch Throwable t
                                       (.completeExceptionally fut t)))))
                 (.start))]
    (doto fut
      (.whenComplete (reify BiConsumer
                       (accept [_ v e]
                         (when-not (instance? InterruptedException e)
                           (.interrupt thread))))))))


(defprotocol PLatestTxId
  "A synchronized box that tracks the latest submitted tx-id."
  (set-latest-tx-id! [this ^Long new-tx-id]
                     "Updates the latest tx-id value iff new-tx-id is higher
                     than the current value.")

  (wait-for-tx-id [this ^Long after-tx-id timeout-ms]
                  "Blocks the calling thread until either the latest tx-id is
                  higher than after-tx-id or the (optional) timeout expires.
                  Returns the latest tx-id that we know about."))


;; A tx-id behind a Lock/Condition.
(deftype LatestTxId [^:volatile-mutable tx-id]
  PLatestTxId
  (set-latest-tx-id! [this new-tx-id]
    (locking this
      (when (> new-tx-id tx-id)
        (set! tx-id new-tx-id)
        (.notifyAll this))
      tx-id))

  (wait-for-tx-id [this after-tx-id timeout-ms]
    (locking this
      (loop [after-tx-id after-tx-id]
        (cond
          ;; If we know that there are new transactions, return immediately.
          (> tx-id after-tx-id)
          tx-id

          ;; If we have a timeout, wait at most that length of time. We don't
          ;; bother detecting early or spurious wakeups.
          timeout-ms
          (do (.wait this timeout-ms)
              tx-id)

          ;; With no timeout, wait indefinitely for tx-id to exceed
          ;; after-tx-id.
          :else
          (do (.wait this)
              (recur after-tx-id)))))))


(defn ->latest-tx-id []
  (->LatestTxId -1))


(defprotocol PSubscriberHandler
  ;; Logically, tx-log should be a property of the subscriber handler, but this
  ;; creates circular references in practice. Passing different tx-log objects
  ;; to the same subscriber handler will result in undefined behavior.
  ;;
  ;; XXX: Can we not use a weak reference or something? Having tx-log in the
  ;; parameter list is a bit awkward.
  (handle-subscriber [this tx-log after-tx-id f]
                     "Starts a thread that will call f with tx records as they
                     become available. f takes a CompletableFuture, which can
                     be used to halt the subscriber, and the next tx. Returns
                     the CompletableFuture.")

  (notify-tx! [this tx]
              "Notifies the handler of a transaction available on the tx-log.
              TxLog backends can call this any time they successfully submit a
              transaction, to immediately wake up the indexer. Some backends
              may also wish to call this in response to their own asynchronous
              notifications. Redundant and out-of-order invocations are quietly
              ignored."))


(defn- try-open-tx-log [tx-log after-tx-id]
  (try
    (db/open-tx-log tx-log after-tx-id)
    (catch InterruptedException e (throw e))
    (catch Exception e
      (log/warn e "Error polling for txs, will retry soon."))))


(defn- tx-handler [^CompletableFuture fut f]
  (fn [_last-tx-id tx]
    (f fut tx)
    (cond-> (::xt/tx-id tx)
      (.isDone fut) (reduced))))


(defrecord SubscriberHandler [!latest-tx-id opts]
  PSubscriberHandler
  (handle-subscriber [this tx-log after-tx-id f]
    (let [poll-timeout-ms (some-> ^Duration (:poll-sleep-duration opts) (.toMillis))
          fut (completable-thread
                (fn [^CompletableFuture fut]
                  (loop [after-tx-id (or after-tx-id -1)
                         wait? true]
                    (when wait?
                      (wait-for-tx-id !latest-tx-id after-tx-id poll-timeout-ms))
                    (let [last-tx-id (when-some [log (try-open-tx-log tx-log after-tx-id)]
                                       (with-open [^ICursor log log]
                                         (reduce (tx-handler fut f) after-tx-id (iterator-seq log))))]
                      (cond
                        (.isDone fut)
                        nil

                        ;; If we couldn't open the log, we'll give it a moment and keep trying.
                        (nil? last-tx-id)
                        (do (Thread/sleep 500)  ;; XXX: What should this be?
                            (recur after-tx-id true))

                        ;; Non-empty result: report last-tx-id and loop immediately.
                        (> last-tx-id after-tx-id)
                        (do (set-latest-tx-id! !latest-tx-id last-tx-id)
                            (recur last-tx-id false))

                        ;; Empty result: wait for more.
                        :else
                        (recur last-tx-id true))))))]

      ;; Prime !latest-tx-id.
      (when-some [tx (db/latest-submitted-tx tx-log)]
        (notify-tx! this tx))

      fut))

  (notify-tx! [_ {::xt/keys [tx-id]}]
    (when (nat-int? tx-id)
      (set-latest-tx-id! !latest-tx-id tx-id))))


(def ^:private default-polling-opts
  {:poll-sleep-duration (Duration/ofMillis 200)})

(def ^:private default-notifying-opts
  {:poll-sleep-duration nil})

(def ^:private default-opts default-polling-opts)


(defn ->subscriber-handler
  "Creates a SubscriberHandler for subscribing to new submitted transactions.

  Options:

    :poll-sleep-duration (java.time.Duration): time between polling for new
      transactions. Defaults to 200 ms. Set to nil to disable polling entirely;
      in this case, the client is entirely responsible for calling notify-tx!
      and/or poll-for-tx!."
  ([]
   (->subscriber-handler {}))

  ([opts]
   (let [opts (merge default-opts opts)]
     (->SubscriberHandler (->latest-tx-id) opts))))


;;
;; Legacy APIs
;;

(defn ->notifying-subscriber-handler
  ([]
   (->subscriber-handler default-notifying-opts))
  ([_latest-submitted-tx]
   (->notifying-subscriber-handler)))

(defn handle-notifying-subscriber [subscriber-handler tx-log after-tx-id f]
  (handle-subscriber subscriber-handler tx-log after-tx-id f))

(defn handle-polling-subscription [tx-log after-tx-id opts f]
  (-> (->subscriber-handler (merge default-polling-opts opts))
      (handle-subscriber tx-log after-tx-id f)))
