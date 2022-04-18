# Ethereum-Analysis
This repository contains the coursework assignment I completed for module Big Data Processing (ECS765P) at Queen Mary University of London in April 2022.

## Ethereum data
The four datasets used for the coursework was stored in an Hadoop Distributed File System (HDFS) cluster at university and only spanned from August 2015 to June 2019. Below I have included the details of each dataset.

### Blocks 

DATASET SCHEMA - BLOCKS

number: The block number
hash: Hash of the block
miner: The address of the beneficiary to whom the mining rewards were given
difficulty: Integer of the difficulty for this block
size: The size of this block in bytes
gas_limit: The maximum gas allowed in this block
gas_used: The total used gas by all transactions in this block
timestamp: The timestamp for when the block was collated
transaction_count: The number of transactions in the block

+-------+--------------------+--------------------+----------------+-----+---------+--------+----------+---------+
| number| hash               |miner               |difficulty|size|gas_limit|gas_used|timestamp|transaction_count|
+-------+--------------------+--------------------+----------------+-----+---------+--------+----------+---------+
|4776199|0x9172600443ac88e...|0x5a0b54d5dc17e0a...|1765656009004680| 9773|  7995996| 2042230|151393
