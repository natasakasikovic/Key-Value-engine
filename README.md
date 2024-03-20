<h1 align="center">Key-Value engine</h1>

This Key-Value Engine is a data storage system that utilizes advanced algorithms and data structures to efficiently manage and retrieve data using key-value pairs.
It has been developed as part of the "Advanced Algorithms and Data Structures" course on the Faculty of Tehnical Sciencies, Novi Sad.
The engine is implemented  in the programing language Golang as a console application enabling user interaction.

Basic Operations:

    PUT(key, value): Adds a new entry into the system. The key is a string, and the value is a byte array.

    GET(key) -> value: Retrieves the value associated with the given key.

    DELETE(key): Deletes the entry associated with the provided key.

Write path:

    1.) Logging data in the commit log
    2.) Writing data to the memtable
    3.) Flushing data from the memtable
    4.) Storing data on disk in SSTables

<p align="center">
  <img src="https://www.scnsoft.com/blog-pictures/business-intelligence/cassandra-performance-3.png" alt="Write path" width="600vw">
</p>

Read path:

    1.) When a user sends a GET request, we first check if the record exists in the Memtable structure 
    (if it does, we return the response).

    2.) After that, we check if the record exists in the Cache structure (if it does, we return the response).

    3.) Next, we check each SSTable structure one by one by loading its Bloom Filter and querying if the key is present. 
    If it is not, we move on to the next SSTable. If it might be, we need to check the remaining structures
    of the current table.

<p align="center">
  <img src="https://www.scnsoft.com/blog-pictures/business-intelligence/cassandra-performance-4.png" alt="Read path" width="600vw">
</p>



## Data structures & algorithms
- [Bloom Filter](https://www.geeksforgeeks.org/bloom-filters-introduction-and-python-implementation/) - implemented by [cofi420](https://github.com/cofi420) & [natasakasikovic](https://github.com/natasakasikovic)
- [Count Min Sketch](https://www.geeksforgeeks.org/count-min-sketch-in-java-with-examples/) - implemented by [MilicMilosRS](https://github.com/MilicMilosRS) & [anasinik](https://github.com/anasinik)
- [Hyper Log Log](https://www.geeksforgeeks.org/complete-tutorial-on-hyperloglog-in-redis/) - implemented by [draganjordanovic](https://github.com/draganjordanovic)
- [SimHash](https://sauravomar01.medium.com/sim-hash-detection-of-duplicate-texts-d5dc2ce2538a) - implemented by [cofi420](https://github.com/cofi420) & [natasakasikovic](https://github.com/natasakasikovic)
- [B-Tree](https://www.geeksforgeeks.org/introduction-of-b-tree-2/) - implemented by [MilicMilosRS](https://github.com/MilicMilosRS)
- [Skip List](https://www.geeksforgeeks.org/skip-list/) - implemented by [anasinik](https://github.com/anasinik)
- [Memtable](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlHowDataWritten.html) - implemented by [draganjordanovic](https://github.com/draganjordanovic)
- [Write Ahead Log](https://medium.com/@abhi18632/understanding-write-ahead-logs-in-distributed-systems-3b36892fa3ba) - implemented by [cofi420](https://github.com/cofi420)
- [Sorted Strings Table](https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html) - implemented by [anasinik](https://github.com/anasinik) & [natasakasikovic](https://github.com/natasakasikovic)
- [Merkle Tree](https://www.geeksforgeeks.org/introduction-to-merkle-tree/) - implemented by [natasakasikovic](https://github.com/natasakasikovic)
- [LRU Cache](https://www.geeksforgeeks.org/lru-cache-implementation/) - implemented by [cofi420](https://github.com/cofi420)
- [Token bucket](https://www.geeksforgeeks.org/token-bucket-algorithm/) - implemented by [cofi420](https://github.com/cofi420)
- [LSM Tree](https://www.scylladb.com/glossary/log-structured-merge-tree/) - implemented by [MilicMilosRS](https://github.com/MilicMilosRS)
