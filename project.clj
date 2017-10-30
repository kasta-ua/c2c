(defproject c2c "1.0.0"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"
            :year 2016
            :key "mit"}
  :dependencies
  [[org.clojure/clojure "1.8.0"]
   [org.clojure/tools.cli "0.3.5"]

   [cc.qbits/alia-all "3.2.0"
    :exclusions [org.clojure/clojure]]
   [cc.qbits/hayt "3.2.0"]

   [org.clojure/java.jdbc "0.6.1"
    :exclusions [mysql/mysql-connector-java
                 org.apache.derby/derby
                 hsqldb
                 org.xerial/sqlite-jdbc
                 net.sourceforge.jtds/jtds]]
   [org.postgresql/postgresql "9.4.1209"]
   [honeysql "0.8.2"]
   [nilenso/honeysql-postgres "0.2.2"]

   [cc.qbits/spandex "0.5.2"]

   [org.clojure/tools.logging "0.3.1"]
   [ch.qos.logback/logback-classic "1.1.7"]
   [spootnik/unilog "0.7.15"]]

  :main c2c.main

  :profiles {:uberjar {:aot :all
                       :uberjar-name "c2c.jar"}})
