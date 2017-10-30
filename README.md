# Simple tool to migrate data from one database to another

* Can copy data between instances of Cassandra and Postgres. Also can rename
fields in the process.

* Can copy index/type between instances of ES, in this case
--from TABLE means index or index/type of source
--to   TABLE means index or index/type of destination


## Installation

* Install lein https://leiningen.org/#install

* Clone repository and run `lein uberjar`.


## Usage

Run `java -jar target/c2c.jar --help`

args:
`--src-conn es://10.38.0.120:9200
--dst-conn es://10.38.0.121:9203
--from product
--to product2
--fetch-size 1000
--insert-size 100`
