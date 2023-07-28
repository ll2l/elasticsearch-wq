esweb
================
A web based elasticsearch query and management tools written in Go.

Overview
========
This is a web-based browser for elasticsearch query/management. Its written in Go and works on Mac OSX, Linux and Windows machines. Main idea behind using Go for the backend is to utilize language's ability for cross-compile source code for multiple platforms. This project is my attempt to learn golang.  
### query
![image](https://github.com/ll2l/esweb/blob/master/docs/esweb-query.JPG)

### management
![image](https://github.com/ll2l/esweb/blob/master/esweb-indexManage.JPG)

## Note
 This is a work in progress. only elasticsearch v6.+ has been tested.


## How to run

1. Download the [precompiled binary](https://github.com/ll2l/esweb/releases) for your operating system.
2. Execute esweb  
  2.1 one cluster: esweb --address http://es_cluster:9200  
  2.2 multi cluster: config bookmark then esweb -b default
3. open http://localhost:8081/ in your browser


## TODO

- Monitor
- Index paste
- Metrics
- Performance index migration
- Query editor autocomplete


## License

The MIT License (MIT)