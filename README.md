# Sequence
---
Multi consumer stream aggregator and processor in rust. 


## Thesis
---

1. Bring computation near data. ETL jobs often expose data at points where it is has value when accessed
   from consumers but requires additional transformations. By bringing transformations to the data but still
   exposing the data to consumers we can efficiently and flexibility. 
2. Transformations and partitions should be opaque to the user. Given the ability to elastically expand compute 
   resources, partitioning and other scaling techniques should be hidden from the user.

## Rough design and key terms

### Topic 

Topics can be published to and read from. When read from, consumers can specify topic filters to only read a subset of 
messages in a topic. Messages in a topic have at least one primary/partition key. Within the topic and partition key, 
the messages are ordered. 

### Transforms 

Link multiple topics together with some computation that is user defined. For now this is a simple rust function but in
the future this will be expanded to be function def in multiple languages/containers. The transform processes one message 
a time for each of its current partitions. To enable scaling, the transform step may re partition and split partitions, 
into new partitions, creating new functions to process the values. This is opaque to the user, apart from provision of 
available key resources. 

Think AWS Kinesis, but messages are published to another topic which can re-read from and there is only one long-lived 
consumer per kinesis partition, which processes the messages. 

1. To support this case data structures used in memory and not using external dbs should support a split partition 
   method. Which splits data structures resources by the partition key, so aggregations are not lost within a key. 

### Consumers 

As topic partitions are opaque to consumers, we need a method of scaling consumers without making them have to be 
partition aware. Solution is consumer group, where consumers in a single group get a subset of each active partition.


## Open Questions and Possible Answers 

#### How should partitions work with consumers and transformers. 

topic partitions should be independent of transformers as they can have different capacity requirements due to writing 
to the topic being written to from multiple sources. Consumer groups/partitions should match that of the topic 
partition. We should scale these quickly. 


#### What should trigger repartition ? 

Back pressure should be the general metric or queue size. 

For transformers, the size of the queue on a particular partition .

For topics, rate of consumer back pressure/ queue factor of active consumers that are not transformers. As transformers 
will handle their own partitioning to accommodate load ? 

Any 


#### Should messages be durable. 

API should allow in memory and durable for each stage that require queueing. 




