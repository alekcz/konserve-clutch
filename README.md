# konserve-clutch

A CouchDB implementation of the [konserve kv-protocol](https://github.com/replikativ/konserve) on top of [clutch](https://github.com/clojure-clutch/clutch).

# Status

![Clojure CI](https://github.com/alekcz/konserve-clutch/workflows/Clojure%20CI/badge.svg?branch=master) [![codecov](https://codecov.io/gh/alekcz/konserve-clutch/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/konserve-clutch) 

## Usage

[![Clojars Project](https://img.shields.io/clojars/v/io.replikativ/konserve-clutch.svg)](http://clojars.org/io.replikativ/konserve-clutch)

`[io.replikativ/konserve-clutch "0.1.4-SNAPSHOT"]`

The purpose of konserve is to have a unified associative key-value interface for
edn datastructures and binary blobs. Use the standard interface functions of konserve.

You can provide the carmine redis connection specification map to the
`new-clutch-store` constructor as an argument. We do not require additional
settings beyond the konserve serialization protocol for the store, so you can
still access the store through carmine directly wherever you need.

```clojure
(require '[konserve-clutch.core :refer :all]
         '[clojure.core.async :refer [<!!] :as async]
         '[konserve.core :as k])
  
  (def clutch-store (<!! (new-clutch-store "http://username:password@localhost:5984/database")))

  (<!! (k/exists? clutch-store  "cecilia"))
  (<!! (k/get-in clutch-store ["cecilia"]))
  (<!! (k/assoc-in clutch-store ["cecilia"] 28))
  (<!! (k/update-in clutch-store ["cecilia"] inc))
  (<!! (k/get-in clutch-store ["cecilia"]))

  (defrecord Test [a])
  (<!! (k/assoc-in clutch-store ["agatha"] (Test. 35)))
  (<!! (k/get-in clutch-store ["agatha"]))
```

## Changes

### 0.1.4
- update to konserve 0.6.0

### 0.1.2

- binary support
- update to konserve 0.4
- arbitrary key length (hashing)

### 0.1.1
- use new reduced konserve interface and serializers

### 0.1.0
- factor out from konserve

## License

Copyright © 2014-2016 Christian Weilbach

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
