(ns konserve-clutch.core
  "CouchDB store implemented with Clutch."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [com.ashafa.clutch :as cl]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]])
  (:import  [java.io StringWriter ByteArrayInputStream]))

(set! *warn-on-reflection* 1)
(def store-layout 1)
(def serializer 1)
(def compressor 0)
(def encryptor 0)

(defn add-header [data]
  (when (seq data) 
    (if (= String (type data))
      (str (char store-layout) (char serializer) (char compressor) (char encryptor) data)
      (byte-array (into [] (concat 
                            [(byte store-layout) (byte serializer) (byte compressor) (byte encryptor)] 
                            (vec data)))))))

(defn strip-header [data]
  (when (seq data) 
    (if (= String (type data))
      (subs data 4)
      (byte-array (->> data vec (split-at 4) second)))))

(defn prep-string-write
  [id data]
  (let [[meta val] data]
    [{:_id id
      :meta (add-header meta)
      :binary false}
      [{:data (add-header (.getBytes ^String val))
        :filename id
        :mime-type "application/octet-stream"}]]))

(defn prep-binary-write 
  [id data]
  (let [[meta val] data]
    [{:_id id
      :meta (add-header meta)
      :binary true}
     [{:data (add-header val)
       :filename id
       :mime-type "application/octet-stream"}]]))

(defn it-exists? 
  [db id]
  (cl/document-exists? db id))
  
(defn get-it 
  [db id]
  (let [doc (cl/get-document db id)
        attachment (cl/get-attachment db id id)]
    [(-> doc :meta strip-header) (when attachment (-> attachment slurp strip-header))]))

(defn get-it-only-string
  [db id]
  (let [attachment (cl/get-attachment db id id)]
    (when attachment 
      (-> attachment slurp strip-header))))

(defn get-it-only-binary
  [db id]
  (let [attachment (cl/get-attachment db id id)]
    (when attachment 
      (->> attachment slurp (map byte) byte-array strip-header))))

(defn get-meta
  [db id]
  (-> (cl/get-document db id) :meta strip-header))

(defn delete-it 
  [db id]
  (let [doc (cl/get-document db id)]
    (when doc (cl/delete-document db doc))))

(defn update-it 
  [db id data]
  (let [[doc attachment] (prep-string-write id data)]
    (delete-it db id)
    (cl/put-document db doc :attachments attachment)))

(defn update-binary
  [db id data]
  (let [[doc attachment] (prep-binary-write id data)]
    (delete-it db id)
    (cl/put-document db doc :attachments attachment)))

(defn get-keys 
  [db]
  (cl/all-documents db))

(defn str-uuid 
  [key] 
  (str (hasch/uuid key))) 

(defn prep-ex 
  [^String message ^Exception e]
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  [attachment]
  { :input-stream (ByteArrayInputStream. attachment)
    :size (count attachment)})

(defrecord ClutchStore [db serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (let [it (it-exists? db (str-uuid key))]
              (async/put! res-ch it))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
        res-ch))

  (-get [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it-only-string db (str-uuid key))]
            (if (some? res)
              (async/put! res-ch (-deserialize serializer read-handlers res))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-meta db (str-uuid key))]
            (if (some? res)
              (async/put! res-ch (-deserialize serializer read-handlers res))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                [ometa' oval'] (get-it db (str-uuid fkey))
                old-val [(when ometa'
                          (-deserialize serializer read-handlers ometa'))
                         (when oval'
                          (-deserialize serializer read-handlers oval'))]            
                [nmeta nval] [(meta-up-fn (first old-val)) 
                         (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                ^StringWriter mbaos (StringWriter.)
                ^StringWriter vbaos (StringWriter.)]
            (when nmeta (-serialize serializer mbaos write-handlers nmeta))
            (when nval (-serialize serializer vbaos write-handlers nval))
            (update-it db (str-uuid fkey) [(.toString mbaos) (.toString vbaos)])
            (async/put! res-ch [(second old-val) nval]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-it db (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it-only-binary db (str-uuid key))]
            (if (some? res)
              (async/put! res-ch (locked-cb (prep-stream res)))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [old-val (get-it db (str-uuid key))
                old-meta (-deserialize serializer read-handlers (first old-val))
                new-meta (meta-up-fn old-meta)
                ^StringWriter mbaos (StringWriter.)]
            (-serialize serializer mbaos write-handlers new-meta)
            (update-binary db (str-uuid key) [(.toString mbaos) input])
            (async/put! res-ch [(second old-val) input]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [key-stream (get-keys db)
                keys' (when key-stream
                        (for [k key-stream]
                          (let [meta (get-meta db (:id k))]
                            (-deserialize serializer read-handlers meta))))
                keys (map :key keys')]
            (doall
              (map #(async/put! res-ch %) keys)))
          (async/close! res-ch) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch)))

(defn new-clutch-store
  "Constructs a clutch CouchDB store either with name for db or a clutch DB
  object and optionally read and write handlers for custom types according to
  incognito and a serialization protocol according to konserve."
  [db & {:keys [serializer read-handlers write-handlers]
         :or {serializer (ser/string-serializer)
              read-handlers (atom {})
              write-handlers (atom {})}}]
  (let [res-ch (async/chan 1)]
    (async/thread 
      (try
        (let [db (if (string? db) (cl/couch db) db)]
          (async/put! res-ch
            (map->ClutchStore {:db (cl/get-database db)
                               :serializer serializer
                               :read-handlers read-handlers
                               :write-handlers write-handlers
                               :locks (atom {})})))
        (catch Exception e (async/put! res-ch (prep-ex "Failed to connect to store" e)))))          
    res-ch))

(defn delete-store [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (cl/delete-database (:db store))
        (async/close! res-ch)
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
    res-ch))


