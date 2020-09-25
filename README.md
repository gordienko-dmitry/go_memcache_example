## GO MEMCACHE EXAMPLE

It's loader specific format files to memcached storage.  

It multi-processing realization.   
For more speed and asynchrony number of processes == number of PC cores.   



### Run
```  
go run memc_load.go --pattern ./data/*.gz  
```  


### All parameters
```  
-log  -- log file (default: log to console)  
-w  --  count of writing processes (default 3)  
-r  --  count of reading processes (default 3)  
--pattern  --  pattern for input files (default: /data/appsinstalled/*.tsv.gz)  

--idfa  -- memcache address for 'idfa' type of input  
--gaid  -- memcache address for 'gaid' type of input  
--adid  -- memcache address for 'adid' type of input  
--avid  -- memcache address for 'avid' type of input  
```  


### Install require  
For this script you'd require install some packages.
```  
brew install protobuf  
protoc  --go_out=. ./appinstalled/appsinstalled.proto  
go get github.com/bradfitz/gomemcache/memcache  
go get github.com/golang/protobuf/proto  
```  

and run some memcached instance (one for each type of input)  

```    
memcached -d -p 33013  
memcached -d -p 33014  
memcached -d -p 33015  
memcached -d -p 33016  
```  


