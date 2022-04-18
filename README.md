# Ethereum-Analysis
This repository contains the coursework assignment I completed for module Big Data Processing (ECS765P) at Queen Mary University of London in April 2022.

## Ethereum data
The four datasets used for the coursework was stored in an Hadoop Distributed File System (HDFS) cluster at university and only spanned from August 2015 to June 2019. Below I have included the details of each dataset.

DATASET SCHEMA - BLOCKS
- number: The block number
- hash: Hash of the block
- miner: The address of the beneficiary to whom the mining rewards were given
- difficulty: Integer of the difficulty for this block
- size: The size of this block in bytes
- gas_limit: The maximum gas allowed in this block
- gas_used: The total used gas by all transactions in this block
- timestamp: The timestamp for when the block was collated
- transaction_count: The number of transactions in the block

DATASET SCHEMA - TRANSACTIONS
- block_number: Block number where this transaction was in
- from_address: Address of the sender
- to_address: Address of the receiver. null when it is a contract creation transaction
- value: Value transferred in Wei (the smallest denomination of ether)
- gas: Gas provided by the sender
- gas_price : Gas price provided by the sender in Wei
- block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)

DATASET SCHEMA - CONTRACTS
- address: Address of the contract
- is_erc20: Whether this contract is an ERC20 contract
- is_erc721: Whether this contract is an ERC721 contract
- block_number: Block number where this contract was created

DATASET SCHEMA - SCAMS.JSON
- id: Unique ID for the reported scam
- name: Name of the Scam
- url: Hosting URL
- coin: Currency the scam is attempting to gain 
- category: Category of scam - Phishing, Ransomware, Trust Trade, etc.
- subcategory: Subdivisions of Category
- description: Description of the scam provided by the reporter and datasource
- addresses: List of known addresses associated with the scam
- reporter: User/company who reported the scam first
- ip: IP address of the reporter
- status: If the scam is currently active, inactive or has been taken offline
