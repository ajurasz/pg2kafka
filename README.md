# pg2kafka

Simple demonstration how to read data from `PostgreSQL` and move it to `Kafka` topic with `go`.

Key features:
- Serialize messages to `Avro`
- Compress messages with `gzip` (to save as much disk space as possible - this will add some overhead for CPU when compressing and uncompressing messages)
- Distribute messages evenly across all partitions with `sarama.NewRoundRobinPartitioner`
- Handle nullable values from DB and proper `Avro` serialization
