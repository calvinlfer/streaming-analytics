# Simple Analytics #

This system provides simple data analytics for a website. This system is able to answer the following 
questions:

- How many unique visitors have visited my site per hour?
- How many clicks per hour?
- How many impressions per hour?

## Running the system ##

The system is comprised of three distinct modules:

- __frontend__: provides ingestion and querying endpoints
- __clicks-and-impressions-stream-processor__: responsible for consuming click and impression events from the journal 
and aggregating them and pushing them to Cassandra
- __unique-users-stream-processor__: responsible for consuming events from the journal, aggregating them and performing 
cardinality estimation using HyperLogLog and pushing them to Cassandra

### Infrastructure ###

This system makes use of Cassandra to store analytics and Kafka for firehosing data and as a distribution mechanism for
decoupling modules. In order to spin up these dependencies, you can use
```bash
docker-compose up
```

#### Production-like setup ####
Now, we will package up the application to be used in a production-like environment:
