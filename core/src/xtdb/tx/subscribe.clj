(ns xtdb.tx.subscribe
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.io :as xio])
  (:import xtdb.api.ICursor
           java.time.Duration
           [java.util.concurrent CompletableFuture ScheduledFuture ScheduledThreadPoolExecutor TimeUnit]
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


(defn- tx-handler [^CompletableFuture fut f]
  (fn [_last-tx-id tx]
    (f fut tx)

    (let [tx-id (::xt/tx-id tx)]
      (cond
        (.isDone fut) (reduced tx-id)
        (Thread/interrupted) (throw (InterruptedException.))
        :else tx-id))))


(defprotocol PLatestTxId
  "A synchronized box that tracks the latest submitted tx-id."
  (set-latest-tx-id! [this ^Long new-tx-id]
                     "Updates the latest tx-id value iff new-tx-id is higher
                     than the current value.")
  (wait-for-tx-id [this ^Long after-tx-id]
                  "Blocks the calling thread until the latest tx-id is higher
                  than after-tx-id. Returns the latest tx-id."))


;; Essentially a tx-id behind a Lock/Condition. This plays nicely with
;; completable threads, as interrupting the thread will pop us out of
;; Object.wait() with an InterruptedException.
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
           (.wait this)
           (recur after-tx-id)))))))


(defn ->latest-tx-id
  ([]
   (->latest-tx-id -1))
  ([tx-id]
   (->LatestTxId tx-id)))


;; Note that some of these methods take a tx-log argument. Logically, this
;; should be a property of the subscriber handler, but this creates circular
;; references in practice. Passing different tx-log objects to the same
;; subscriber handler will result in undefined behavior.
;;
;; XXX: Can we not use a weak reference or something? Having tx-log in the
;; parameter lists is weird in principle and awkward in practice.
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


(defprotocol PPollTimer
  (acquire-timer! [this handler tx-log opts]
                  "Ensures that this polling timer is running. Returns a lease
                  value that must be presented later to release-timer!.")

  (release-timer! [this lease]
                  "Balances a previous call to acquire-timer! When all leases
                  are released, the timer is canceled."))


;; In pure Clojure, I might do this with an agent. XT seems to prefer Java
;; concurrency primitives, so we'll do it the Java way.
(deftype PollTimer [^ScheduledThreadPoolExecutor executor, ^:volatile-mutable leases, ^:volatile-mutable ^ScheduledFuture fut]
  PPollTimer
  (acquire-timer! [this handler tx-log {:keys [^Duration poll-sleep-duration]}]
    (locking this
      (let [lease (gensym "poll-lease-")]
        (set! leases (conj leases lease))
        (when (nil? fut)
          (set! fut (.scheduleWithFixedDelay executor
                                             (reify Runnable (run [_] (poll-for-tx! handler tx-log)))
                                             (.toMillis poll-sleep-duration)
                                             (.toMillis poll-sleep-duration)
                                             TimeUnit/MILLISECONDS)))
        lease)))

  (release-timer! [this lease]
    (locking this
      (if-not (leases lease)
        (log/warn (str "Timer lease not found: " lease))
        (do
          (set! leases (disj leases lease))
          (when (and (empty? leases) (some? fut))
            (.cancel fut false)
            (set! fut nil)))))))


(defn ->poll-timer []
  (->PollTimer (ScheduledThreadPoolExecutor. 1) #{} nil))


(defrecord SubscriberHandler [!latest-tx-id !poll-timer opts]
  PSubscriberHandler
  (handle-subscriber [this tx-log after-tx-id f]
    (let [{:keys [poll-sleep-duration]} opts
          ;; Start processing txs.
          fut (completable-thread
                (fn [^CompletableFuture fut]
                  (loop [after-tx-id after-tx-id]
                    (wait-for-tx-id !latest-tx-id after-tx-id)
                    (let [last-tx-id (with-open [^ICursor log (db/open-tx-log tx-log after-tx-id)]
                                         (reduce (tx-handler fut f)
                                                 after-tx-id
                                                 (iterator-seq log)))]
                       (cond
                         (.isDone fut) nil
                         (Thread/interrupted) (throw (InterruptedException.))
                         :else (recur last-tx-id))))))]

      ;; Set up the timer, if configured.
      (when poll-sleep-duration
        (let [lease (acquire-timer! !poll-timer this tx-log opts)]
          (.whenComplete fut (reify BiConsumer (accept [_ v e] (release-timer! !poll-timer lease))))))

      ;; Prime !latest-tx-id.
      (poll-for-tx! this tx-log)

      fut))

  (notify-tx! [_ {::xt/keys [tx-id]}]
    (when (nat-int? tx-id)
      (set-latest-tx-id! !latest-tx-id tx-id)))

  (poll-for-tx! [this tx-log]
    (some->> (db/latest-submitted-tx tx-log)
      (notify-tx! this))))


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
     (->SubscriberHandler (->latest-tx-id) (->poll-timer) opts))))


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
