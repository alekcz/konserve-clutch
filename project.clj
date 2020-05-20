(defproject io.replikativ/konserve-clutch "0.1.3"
  :description "A CouchDB backend for konserve with clutch."
  :url "https://github.com/replikativ/konserve-clutch"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0" :scope "provided"]
                 [com.ashafa/clutch "0.4.0"]
                 [io.replikativ/konserve "0.6.0-20200512.093105-1"]]
  :plugins [[lein-cloverage "1.1.2"]]                
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-20200404.091302-14"]]}})
