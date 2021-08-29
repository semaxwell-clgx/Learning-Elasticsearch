# Querying Elasticsearch with Java
This code repo is supplemental to the Udemy course [Complete Elasticsearch Masterclass with Logstash and Kibana](https://cognizant.udemy.com/course/complete-elasticsearch-masterclass-with-kibana-and-logstash).

While the Udemy course does a good job of introducing Elasticsearch/kibana querying, the focus of this repo is on learning how to implement those queries in Java. Some knowledge of Elasticsearch queries is expected.

There are some additional queries not covered in the course, particularly around nested objects.

## Step 1: Setup Local Elasticsearch & Kibana

#### Setup
    docker network create elastic
    docker run -d --name es01-test --net elastic -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.12.1
    docker run -d --name kib01-test --net elastic -p 5601:5601 -e "ELASTICSEARCH_HOSTS=http://es01-test:9200" docker.elastic.co/kibana/kibana:7.12.1

#### To Stop the containers
    docker stop es01-test
    docker stop kib01-test

#### To remove the containers and network
    docker network rm elastic
    docker rm es01-test
    docker rm kib01-test

You should now be able to access Kibana at localhost:5601

## Step 2: Populate the (implicit) index with data
Note: if desired, you can use _src.test.java.com.elasticsearch.DataLoader_ to avoid loading data manually.
```json
PUT /courses/_doc/1
{
  "name": "Accounting 101",
  "room": "E3",
  "professor": {
    "name": "Thomas Baszo",
    "department": "finance",
    "faculty_type": "part-time",
    "email": "baszot@onuni.com"
  },
  "students_enrolled": 27,
  "course_publish_date": "2015-01-19",
  "course_description": "Act 101 is a course from the business school on the introduction to accounting that teaches students how to read and compose basic financial statements"
}
PUT /courses/_doc/2
{
  "name": "Marketing 101",
  "room": "E4",
  "professor": {
    "name": "William Smith",
    "department": "finance",
    "faculty_type": "part-time",
    "email": "wills@onuni.com"
  },
  "students_enrolled": 18,
  "course_publish_date": "2015-06-21",
  "course_description": "Mkt 101 is a course from the business school on the introduction to marketing that teaches students the fundamentals of market analysis, customer retention and online advertisements"
}
PUT /courses/_doc/3
{
  "name": "Anthropology 230",
  "room": "G11",
  "professor": {
    "name": "Devin Cranford",
    "department": "history",
    "faculty_type": "full-time",
    "email": "devinc@onuni.com"
  },
  "students_enrolled": 22,
  "course_publish_date": "2013-08-27",
  "course_description": "Ant 230 is an intermediate course on human societies and cultures and their development. A focus on the Mayans civilization is rooted in this course"
}
PUT /courses/_doc/4
{
  "name": "Computer Science 101",
  "room": "C12",
  "professor": {
    "name": "Gregg Payne",
    "department": "engineering",
    "faculty_type": "full-time",
    "email": "payneg@onuni.com"
  },
  "students_enrolled": 33,
  "course_publish_date": "2013-08-27",
  "course_description": "CS 101 is a first year computer science introduction teaching fundamental data structures and alogirthms using python. "
}
PUT /courses/_doc/5
{
  "name": "Theatre 410",
  "room": "T18",
  "professor": {
    "name": "Sebastian Hern",
    "department": "art",
    "faculty_type": "part-time"
  },
  "students_enrolled": 47,
  "course_publish_date": "2013-01-27",
  "course_description": "Tht 410 is an advanced elective course disecting the various plays written by shakespere during the 16th century"
}
PUT /courses/_doc/6
{
  "name": "Cost Accounting 400",
  "room": "E7",
  "professor": {
    "name": "Bill Cage",
    "department": "accounting",
    "faculty_type": "full-time",
    "email": "cageb@onuni.com"
  },
  "students_enrolled": 31,
  "course_publish_date": "2014-12-31",
  "course_description": "Cst Act 400 is an advanced course from the business school taken by final year accounting majors that covers the subject of business incurred costs and how to record them in financial statements"
}
PUT /courses/_doc/7
{
  "name": "Computer Internals 250",
  "room": "C8",
  "professor": {
    "name": "Gregg Payne",
    "department": "engineering",
    "faculty_type": "part-time",
    "email": "payneg@onuni.com"
  },
  "students_enrolled": 33,
  "course_publish_date": "2012-08-20",
  "course_description": "cpt Int 250 gives students an integrated and rigorous picture of applied computer science, as it comes to play in the construction of a simple yet powerful computer system. "
}
PUT /courses/_doc/8
{
  "name": "Accounting Info Systems 350",
  "room": "E3",
  "professor": {
    "name": "Bill Cage",
    "department": "accounting",
    "faculty_type": "full-time",
    "email": "cageb@onuni.com"
  },
  "students_enrolled": 19,
  "course_publish_date": "2014-05-15",
  "course_description": "Act Sys 350 is an advanced course providing students a practical understanding of an accounting system in database technology. Students will use MS Access to build a transaction ledger system"
}
PUT /courses/_doc/9
{
  "name": "Tax Accounting 200",
  "room": "E7",
  "professor": {
    "name": "Thomas Baszo",
    "department": "finance",
    "faculty_type": "part-time",
    "email": "baszot@onuni.com"
  },
  "students_enrolled": 17,
  "course_publish_date": "2016-06-15",
  "course_description": "Tax Act 200 is an intermediate course covering various aspects of tax law"
}
PUT /courses/_doc/10
{
  "name": "Capital Markets 350",
  "room": "E3",
  "professor": {
    "name": "Thomas Baszo",
    "department": "finance",
    "faculty_type": "part-time",
    "email": "baszot@onuni.com"
  },
  "students_enrolled": 13,
  "course_publish_date": "2016-01-11",
  "course_description": "This is an advanced course teaching crucial topics related to raising capital and bonds, shares and other long-term equity and debt financial instrucments"
}
```

## Step 3: Implementing queries in Java
To query Elasticsearch from Java, we can use the following pattern (described [here](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search.html#)):
```java
/* 1. Create a SearchSourceBuilder with your query */
SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
sourceBuilder.query(/* Search query */);

/* 2. Create a SearchRequest, give it an index, and add the SearchSourceBuilder */
SearchRequest searchRequest = new SearchRequest("index");
searchRequest.source(sourceBuilder);

/* 3. Execute the request and capture the response */
HighLevelRestClient client = ...
SearchResponse response = client.search(sourceBuilder);
```
There are two ways to create queries:
1. Using the appropriate builder (e.g. TermQueryBuilder for terms, RangeQueryBuilder for ranges)
2. Using the QueryBuilders class, which supports building multiple types of queries.

See the [Elasticsearch Building Queries](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-query-builders.html) documentation for specifics.


With this in mind, implement the following queries in Java code. Open _src/test/java/com/elasticsearch/ElasticsearchTest.java_ and implement the following queries.

_HINT: You can run the queries in Elasticsearch to see the results you can expect to be returned._

_HINT: You can sysout the SearchSourceBuilder instance to view the Elasticsearch query_

#### 3.a Learning Goal: match_all
Description: Get all course documents
```json
GET /courses/_search
{
  "query":{
    "match_all":{}
  }
}
```

#### 3.b Learning Goal: Exists
Description: Get all course documents that have the field 'professor.email'
```json
GET courses/_search
{
  "query":{
    "exists":{
      "field":"professor.email"
    }
  }
}
```

#### 3.c Learning Goal: Match
Description: Get all course documents that have the name property = 'computer'
```json
GET /courses/_search
{
  "query":{
    "match":{
      "name":"computer"
    }
  }
}
```

#### 3.d Learning Goal: Must
Description: search by multiple criteria (logical AND)
```json
GET /courses/_search
{
  "query": {
    "bool": {
      "must": [
        {"match": { "name" : "computer"} },
        {"match": { "room" : "c8"} }
      ]
    }
  }
}
```

#### 3.e Learning Goal: Minimum_should_match
Description: Using 'should' and 'minimum_should_match', we can determine that at least 'n' criteria should match
```json
GET /courses/_search
{
  "query": {
    "bool": {
      "should": [
        {"match": { "name" : "accounting"} },
        {"match": { "room" : "e3"} },
        {"match": { "name" : "computer"} },
        {"match": { "professor.name": "gregg" } }
      ],
      "minimum_should_match": 2
    }
  }
}

```

#### 3.f Learning Goal: multi_match
Description: Match based on A or B (those that meet both have higher relevance).
```json
GET /courses/_search
{
  "query" : {
    "multi_match" : {
      "fields": ["name", "professor.department"],
      "query": "accounting"
    }
  }
}
```

#### 3.g Learning Goal: match_phrase
Description: Match based on a phrase with complete tokens
```json
GET /courses/_search
{
  "query" : {
    "match_phrase" : {
      "course_description": "from the business school taken by"
    }
  }
}
```

#### 3.h Learning Goal: match_phrase_prefix
Description: Match based on phrase with a partial token
```json
GET /courses/_search
{
  "query" : {
    "match_phrase_prefix": {
      "course_description": "from the business school taken by fin"
    }
  }
}
```

#### 3.i Learning Goal: range
Description: Find courses with more than 9 but less than 31 students enrolled.
```json
GET /courses/_search
{
  "query":{
    "range":{
      "students_enrolled":{
        "gte":10,
        "lte":30
      }
    }
  }
}
```


#### 3.j Learning Goal: Putting it all together
Description: Find all courses with the name 'accounting', that are not in room e7, and that have between 10 and 20 students enrolled (inclusive).
```json
GET /courses/_search
{
  "query":{
    "bool":{
      "must":[
        {
          "match":{
            "name":"accounting"
          }
        }
      ],
      "must_not":[
        {
          "match":{
            "room":"e7"
          }
        }
      ],
      "should":[
        {
          "range":{
            "students_enrolled":{
              "gte":10,
              "lte":20
            }
          }
        }
      ],
      "minimum_should_match":1
    }
  }
}
```

### Step 4: Implement data filter queries in Java
4.a
Learning Goal: filter
```json
GET /courses/_search
{
  "query": {
    "bool": {
      "filter": {
        "bool": {
          "must": [
            { "match": { "professor.name": "bill" }},
            { "match": { "name": "accounting" }}
          ]
        }
      },
      "must": [
        {"match": {"room": "e3"}}
      ]
    }
  }
}
```

### Step 5: Nested Queries

#### Create a new index with a nested property
```json
PUT my-users
{
  "mappings": {
    "properties": {
      "user": {
        "type": "nested"
      }
    }
  }
}
```
#### Add some data
```json
PUT my-users/_doc/1
{
  "group" : "fans",
  "user" : [
    {
      "first" : "John",
      "last" :  "Smith"
    },
    {
      "first" : "Alice",
      "last" :  "White"
    }
  ]
}
```
#### 5.a Learning Goal: nested
Description: uses a nested query to retrieve the correct user
```json
GET my-users/_search
{
  "query": {
    "nested": {
      "path": "user",
      "query": {
        "bool": {
          "must": [
            { "match": { "user.first": "Alice" }},
            { "match": { "user.last":  "White" }}
          ]
        }
      },
      "inner_hits": {
        "highlight": {
          "fields": {
            "user.first": {}
          }
        }
      }
    }
  }
}
```


### Nested nested queries
#### Create the 'Drivers' index
```json
PUT /drivers
{
  "mappings":{
    "properties":{
      "driver":{
        "type":"nested",
        "properties":{
          "last_name":{
            "type":"text"
          },
          "vehicle":{
            "type":"nested",
            "properties":{
              "make":{
                "type":"text"
              },
              "model":{
                "type":"text"
              }
            }
          }
        }
      }
    }
  }
}
```
#### Add Drivers
```json
PUT /drivers/_doc/1
{
  "driver":{
    "last_name":"McQueen",
    "vehicle":[
      {
        "make":"Powell Motors",
        "model":"Canyonero"
      },
      {
        "make":"Miller-Meteor",
        "model":"Ecto-1"
      }
    ]
  }
}
PUT /drivers/_doc/2
{
  "driver":{
    "last_name":"Hudson",
    "vehicle":[
      {
        "make":"Mifune",
        "model":"Mach Five"
      },
      {
        "make":"Miller-Meteor",
        "model":"Ecto-1"
      }
    ]
  }
}
```
#### Search using multi-level query
```json
GET /drivers/_search
{
  "query":{
    "nested":{
      "path":"driver",
      "query":{
        "nested":{
          "path":"driver.vehicle",
          "query":{
            "bool":{
              "must":[
                {
                  "match":{
                    "driver.vehicle.make":"Powell Motors"
                  }
                },
                {
                  "match":{
                    "driver.vehicle.model":"Canyonero"
                  }
                }
              ]
            }
          }
        }
      }
    }
  }
}
```

### Step 6: Pagination, aggregations, and other miscellanea

#### Create the index 'vehicles' and add bulk data
```json
POST /vehicles/_bulk
{ "index": {}}
{ "price" : 10000, "color" : "white", "make" : "honda", "sold" : "2016-10-28", "condition": "okay"}
{ "index": {}}
{ "price" : 20000, "color" : "white", "make" : "honda", "sold" : "2016-11-05", "condition": "new" }
{ "index": {}}
{ "price" : 30000, "color" : "green", "make" : "ford", "sold" : "2016-05-18", "condition": "new" }
{ "index": {}}
{ "price" : 15000, "color" : "blue", "make" : "toyota", "sold" : "2016-07-02", "condition": "good" }
{ "index": {}}
{ "price" : 12000, "color" : "green", "make" : "toyota", "sold" : "2016-08-19" , "condition": "good"}
{ "index": {}}
{ "price" : 18000, "color" : "red", "make" : "dodge", "sold" : "2016-11-05", "condition": "good"  }
{ "index": {}}
{ "price" : 80000, "color" : "red", "make" : "bmw", "sold" : "2016-01-01", "condition": "new"  }
{ "index": {}}
{ "price" : 25000, "color" : "blue", "make" : "ford", "sold" : "2016-08-22", "condition": "new"  }
{ "index": {}}
{ "price" : 10000, "color" : "gray", "make" : "dodge", "sold" : "2016-02-12", "condition": "okay" }
{ "index": {}}
{ "price" : 19000, "color" : "red", "make" : "dodge", "sold" : "2016-02-12", "condition": "good" }
{ "index": {}}
{ "price" : 20000, "color" : "red", "make" : "chevrolet", "sold" : "2016-08-15", "condition": "good" }
{ "index": {}}
{ "price" : 13000, "color" : "gray", "make" : "chevrolet", "sold" : "2016-11-20", "condition": "okay" }
{ "index": {}}
{ "price" : 12500, "color" : "gray", "make" : "dodge", "sold" : "2016-03-09", "condition": "okay" }
{ "index": {}}
{ "price" : 35000, "color" : "red", "make" : "dodge", "sold" : "2016-04-10", "condition": "new" }
{ "index": {}}
{ "price" : 28000, "color" : "blue", "make" : "chevrolet", "sold" : "2016-08-15", "condition": "new" }
{ "index": {}}
{ "price" : 30000, "color" : "gray", "make" : "bmw", "sold" : "2016-11-20", "condition": "good" }
```


#### 6.a Learning Goal: pagination
Description: grab the first 5 vehicles, sorted by price descending. Note: pagination doesn't affect the hits
```json
GET /vehicles/_search
{
  "from": 0,
  "size": 5,
  "query": {
    "match_all": {}
  },
  "sort": [
    {"price": {"order": "desc"}}
  ]
}
```

#### 6.b Learning Goal: count
Description: count the number of cars with make=model
```json
GET /vehicles/_count
{
  "query": {
    "match": {
      "make": "toyota"
    }
  }
}
```

#### 6.c Learning Goal: aggregation
Description: acquire the total count of cars per make
```json
GET /vehicles/_search
{
  "aggs": {
    "popular_cars": {
      "terms": {
        "field": "make.keyword"
      }
    }
  }
}
```

#### 6.d Learning Goal: aggregation by numeric field
Description: Get average, max, min price per manufacturer. Don't need to use '.keyword' because it's a numeric field
```json
GET /vehicles/_search
{
  "aggs": {
    "popular_cars": {
      "terms": {
        "field": "make.keyword"
      },
      "aggs": {
        "avg_price": {
          "avg": {
            "field": "price"
          }
        },
        "max_price": {
          "max": {
            "field": "price"
          }
        },
        "min_price": {
          "min": {
            "field": "price"
          }
        }
      }
    }
  }
}
```

### 6.e Learning Goal: putting it all together
Description: Narrow the scope of the previous query by adding a query requiring color to be red. Optionally, add '"size": 0' to limit the hits (but not the aggregations)
```json
GET /vehicles/_search
{
  "size": 0,
  "query": {
    "match": {
      "color": "red"
    }
  },
  "aggs": {
    "popular_cars": {
      "terms": {
        "field": "make.keyword"
      },
      "aggs": {
        "avg_price": {
          "avg": {
            "field": "price"
          }
        },
        "max_price": {
          "max": {
            "field": "price"
          }
        },
        "min_price": {
          "min": {
            "field": "price"
          }
        }
      }
    }
  }
}
```

### 6.f Learning Goal: stats
Description: use 'stats' to get min/max/avg/sum
```json
GET /vehicles/_search
{
  "aggs": {
    "popular_cars": {
      "terms": {
        "field": "make.keyword"
      },
      "aggs": {
        "stats_on_price": {
          "stats": {
            "field": "price"
          }
        }
      }
    }
  }
}
```

### 6.f Learning Goal: wrappedQuery
Description: perform the following search by passing a json string to the query builder
```json
GET /courses/_search
{
  "query":{
    "match_all": {}
  }
}
```