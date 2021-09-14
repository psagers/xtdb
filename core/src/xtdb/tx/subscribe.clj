(ns xtdb.tx.subscribe
  (:require [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.io :as xio])
  (:import xtdb.api.ICursor
           java.time.Duration
           [java.util.concurrent CompletableFuture]
           java.util.function.BiConsumer))

(def ^java.util.concurrent.ThreadFactory subscription-thread-factory
  (xio/thread-factory "xtdb-tx-subscription"))

(defn completable-thread [f]
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


(defn- tx-handler [^CompletableFuture fut f]
  (fn [_last-tx-id tx]
    (f fut tx)

    (let [tx-id (::xt/tx-id tx)]
      (cond
        (.isDone fut) (reduced tx-id)
        (Thread/interrupted) (throw (InterruptedException.))
        :else tx-id))))


(defn through-tx-id
  "Returns a transducer that passes through tx maps up to and including one
  with last-tx-id. You can almost do this with take-while, except that
  take-while won't terminate until it sees a non-matching value (which may
  never arrive if no one is submitting txs). This terminates as soon as it sees
  final-tx-id."
  [final-tx-id]
  (fn [rf]
    (fn ([] (rf))
        ([result] (rf result))
        ([result {::xt/keys [tx-id] :as tx}]
         (cond
           (< tx-id final-tx-id) (rf result tx)
           (= tx-id final-tx-id) (ensure-reduced (rf result tx))
           :else (ensure-reduced result))))))


(defprotocol PLatestTxId
  "A synchronized box that tracks the latest submitted tx-id."
  (set-latest-tx-id! [this ^Long new-tx-id]
                     "Updates the latest tx-id value iff new-tx-id is higher
                     than the current value.")
  (wait-for-tx-id [this ^Long after-tx-id]
                  "Blocks the calling thread until the latest tx-id is higher
                  than after-tx-id. Returns the latest tx-id."))


;; Essentially a tx-id behind a Lock/Condition.
(deftype LatestTxId [^:volatile-mutable tx-id]
  PLatestTxId
  (set-latest-tx-id! [this new-tx-id]
    (locking this
      (when (> new-tx-id tx-id)
        (set! tx-id new-tx-id)
        (.notifyAll this))
      tx-id))

  (wait-for-tx-id [this after-tx-id]
    (locking this
      (loop [after-tx-id (or after-tx-id -1)]
        (if (> tx-id after-tx-id)
         tx-id
         (do
           (.wait this 1000)
           (if (.isInterrupted (Thread/currentThread))
             tx-id
             (recur after-tx-id))))))))


(defn ->latest-tx-id
  [tx-id]
  (->LatestTxId tx-id))


;; Note that some of these methods take a tx-log argument. Logically, this
;; should be a property of the subscriber handler, but this creates circular
;; references in practice. Passing different tx-log values to the same
;; subscriber handler will result in undefined behavior.
(defprotocol PSubscriberHandler
  (handle-subscriber [this tx-log after-tx-id f]
                     "Starts a thread that will call f with tx records as they
                     become available. f takes a CompletableFuture, which can
                     be used to halt the subscriber, and the next tx. Returns
                     the CompletableFuture.")

  (notify-tx! [this tx]
              "Notifies the handler of the latest transaction available on the
              tx-log. Embedded implementations and those that are able to
              receive notifications from their backends may call this any time
              a new transaction has been recorded.")

  (poll-for-tx! [this tx-log]
                "Polls the tx-log for the latest submited tx and calls
                notify-tx!. This is normally called internally, but clients are
                welcome to set up their own polling loops if they so desire."))


(defn- polling-fn [handler tx-log ^Duration poll-sleep-duration]
  (fn [^CompletableFuture fut]
    (loop []
      (Thread/sleep (.toMillis poll-sleep-duration))
      (cond
        (.isDone fut) nil
        (Thread/interrupted) (throw (InterruptedException.))
        :else (do (poll-for-tx! handler tx-log)
                  (recur))))))


(defrecord SubscriberHandler [!latest-tx-id opts]
  PSubscriberHandler
  (handle-subscriber [this tx-log after-tx-id f]
    (let [{:keys [poll-sleep-duration]} opts

          ;; Optionally enable the polling thread.
          poll-fut (when poll-sleep-duration
                     (completable-thread (polling-fn this tx-log poll-sleep-duration)))

          ;; Start processing txs.
          fut (completable-thread
                (fn [^CompletableFuture fut]
                  (loop [after-tx-id after-tx-id]
                    (let [latest-tx-id (wait-for-tx-id !latest-tx-id after-tx-id)
                          last-tx-id (with-open [^ICursor log (db/open-tx-log tx-log after-tx-id)]
                                         (reduce (tx-handler fut f)
                                                 after-tx-id
                                                 (eduction (through-tx-id latest-tx-id)
                                                           (iterator-seq log))))]
                       (cond
                         (.isDone fut) nil
                         (Thread/interrupted) (throw (InterruptedException.))
                         :else (recur last-tx-id))))))]

      ;; Make sure to tear down the polling thread with the main one.
      (when poll-fut
        (.whenComplete fut (reify BiConsumer (accept [_ v e] (.complete poll-fut nil)))))

      ;; Prime !latest-tx-id.
      (poll-for-tx! this tx-log)

      fut))

  (notify-tx! [_ {::xt/keys [tx-id]}]
    (when (nat-int? tx-id)
      (set-latest-tx-id! !latest-tx-id tx-id)))

  (poll-for-tx! [this tx-log]
    (some->> (db/latest-submitted-tx tx-log)
      (notify-tx! this))))


(def ^:private default-opts
  {:poll-sleep-duration (Duration/ofMillis 200)})


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
     (->SubscriberHandler (->latest-tx-id -1) opts))))


;;
;; Legacy
;;

(defn handle-polling-subscription [tx-log after-tx-id opts f]
  (-> (->subscriber-handler opts)
      (handle-subscriber tx-log after-tx-id f)))
