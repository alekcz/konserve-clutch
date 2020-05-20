(ns konserve-clutch.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [konserve-clutch.core :refer [new-clutch-store delete-store]]
            [malli.generator :as mg])
  (:import  [clojure.lang ExceptionInfo]))

(def username "admin")
(def password "password")

(deftest get-nil-test
  (testing "Test getting on empty store"
    (let [_ (println "Getting from an empty store")
          db "nil-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))]
      (is (= nil (<!! (k/get store :foo))))
      (is (= nil (<!! (k/get-meta store :foo))))
      (is (not (<!! (k/exists? store :foo))))
      (is (= :default (<!! (k/get-in store [:fuu] :default))))
      (<!! (k/bget store :foo (fn [res] 
                                (is (nil? res))))))))

(deftest write-value-test
  (testing "Test writing to store"
    (let [_ (println "Writing to store")
          db "write-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))]
      (is (not (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :bar))
      (is (<!! (k/exists? store :foo)))
      (is (= :bar (<!! (k/get store :foo))))
      (is (= :foo (:key (<!! (k/get-meta store :foo)))))
      (<!! (k/assoc-in store [:baz] {:bar 42}))
      (is (= 42 (<!! (k/get-in store [:baz :bar]))))
      (delete-store store))))

(deftest update-value-test
  (testing "Test updating values in the store"
    (let [_ (println "Updating values in the store")
          db "update-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))]
      (<!! (k/assoc store :foo :baritone))
      (is (= :baritone (<!! (k/get-in store [:foo]))))
      (<!! (k/update-in store [:foo] name))
      (is (= "baritone" (<!! (k/get-in store [:foo]))))
      (delete-store store))))

(deftest exists-test
  (testing "Test check for existing key in the store"
    (let [_ (println "Checking if keys exist")
          db "exists-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))]
      (is (not (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :baritone))
      (is  (<!! (k/exists? store :foo)))
      (<!! (k/dissoc store :foo))
      (is (not (<!! (k/exists? store :foo))))
      (delete-store store))))

(deftest binary-test
  (testing "Test writing binary date"
    (let [_ (println "Reading and writing binary data")
          db "binary-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))]
      ;(is (not (<!! (k/exists? store :binbar))))
      ;(<!! (k/bget store :binbar (fn [ans] (is (nil? ans)))))
      (<!! (k/bassoc store :binbar (byte-array (range 10))))
      (<!! (k/bget store :binbar (fn [{:keys [input-stream] :as resp}]
                                    (println resp)
                                    (is (= (map byte (slurp input-stream))
                                           (range 10))))))
      ; (<!! (k/bassoc store :binbar (byte-array (map inc (range 10))))) 
      ; (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
      ;                               (is (= (map byte (slurp input-stream))
      ;                                      (map inc (range 10)))))))                                          
      ; (is (<!! (k/exists? store :binbar)))
      (delete-store store))))
  
(deftest key-test
  (testing "Test getting keys from the store"
    (let [_ (println "Getting keys from store")
          db "key-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))]
      (is (= #{} (<!! (async/into #{} (k/keys store)))))
      (<!! (k/assoc store :baz 20))
      (<!! (k/assoc store :binbar 20))
      (is (= #{:baz :binbar} (<!! (async/into #{} (k/keys store)))))
      (delete-store store))))  

(deftest append-test
  (testing "Test the append store functionality."
    (let [_ (println "Appending to store")
          db "append-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))]
      (<!! (k/append store :foo {:bar 42}))
      (<!! (k/append store :foo {:bar 43}))
      (is (= (<!! (k/log store :foo))
             '({:bar 42}{:bar 43})))
      (is (= (<!! (k/reduce-log store
                              :foo
                              (fn [acc elem]
                                (conj acc elem))
                              []))
             [{:bar 42} {:bar 43}]))
      (delete-store store))))

(deftest invalid-store-test
  (testing "Invalid store functionality."
    (let [_ (println "Connecting to invalid store")
          db "invalid-test"
          conn1 (str "http://error:error@localhost:5984/" db)
          conn2 (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn1))
          store2 (<!! (new-clutch-store conn2))]
      (is (= ExceptionInfo (type store)))
      (is (not= ExceptionInfo (type store2))))))


(def home
  [:map
    [:name string?]
    [:description string?]
    [:rooms pos-int?]
    [:capacity float?]
    [:address
      [:map
        [:street string?]
        [:number int?]
        [:country [:enum "kenya" "lesotho" "south-africa" "italy" "mozambique" "spain" "india" "brazil" "usa" "germany"]]]]])

(deftest realistic-test
  (testing "Realistic data test."
    (let [_ (println "Entering realistic data")
          db "realistic-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))
          home (mg/generate home {:size 20 :seed 2})
          address (:address home)
          addressless (dissoc home :address)
          name (mg/generate keyword? {:size 15 :seed 3})
          num1 (mg/generate pos-int? {:size 5 :seed 4})
          num2 (mg/generate pos-int? {:size 5 :seed 5})
          floater (mg/generate float? {:size 5 :seed 6})]
      
      (<!! (k/assoc store name addressless))
      (is (= addressless 
             (<!! (k/get store name))))

      (<!! (k/assoc-in store [name :address] address))
      (is (= home 
             (<!! (k/get store name))))

      (<!! (k/update-in store [name :capacity] * floater))
      (is (= (* floater (:capacity home)) 
             (<!! (k/get-in store [name :capacity]))))  

      (<!! (k/update-in store [name :address :number] + num1 num2))
      (is (= (+ num1 num2 (:number address)) 
             (<!! (k/get-in store [name :address :number]))))             
      
      (delete-store store))))   

(deftest bulk-test
  (testing "Bulk data test."
    (let [_ (println "Writing bulk data")
          db "bulk-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))
          string4MB (apply str (repeat 4194304 "7"))
          range2MB 2097152
          sevens (repeat range2MB 7)]
      (print "\nWriting 4MB string: ")
      (time (<!! (k/assoc store :record string4MB)))
      (is (= (count string4MB) (count (<!! (k/get store :record)))))
      (print "Writing 2MB binary: ")
      (time (<!! (k/bassoc store :binary (byte-array sevens))))
      (<!! (k/bget store :binary (fn [{:keys [input-stream]}]
                                    (is (= (pmap byte (slurp input-stream))
                                           sevens)))))
      (delete-store store))))  

(deftest exceptions-test
  (testing "Test exception handling"
    (let [_ (println "Generating exceptions")
          db "exceptions-test"
          conn (str "http://" username ":" password "@localhost:5984/" db)
          store (<!! (new-clutch-store conn))
          corrupt (assoc store :db {})] ; let's corrupt our store
      (is (= ExceptionInfo (type (<!! (k/get corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/get-meta corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc corrupt :bad 10)))))
      (is (= ExceptionInfo (type (<!! (k/dissoc corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc-in corrupt [:bad :robot] 10)))))
      (is (= ExceptionInfo (type (<!! (k/update-in corrupt [:bad :robot] inc)))))
      (is (= ExceptionInfo (type (<!! (k/exists? corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/keys corrupt)))))
      (is (= ExceptionInfo (type (<!! (k/bget corrupt :bad (fn [_] nil))))))   
      (is (= ExceptionInfo (type (<!! (k/bassoc corrupt :binbar (byte-array (range 10)))))))   
      (is (= ExceptionInfo (type (<!! (delete-store corrupt)))))
      (delete-store store))))