import pyspark
from operator import add

sc = pyspark.SparkContext()

# checks the blocks dataset
def line_check(line):
    try:
        # splits dataset into fields
        fields = line.split(',')
        if len(fields) == 9:
            # miner address
            str(fields[2])
            # makes sure block size >0
            if int(fields[4]) == 0:
                return False
        else:
            return False
        return True
    except:
        return False

# function takes the blocks dataset and extracts the miner address and blocksize
def addr_blocksize(line):
    try:
        fields = line.split(',')
        if len(fields) == 9:
            addr = fields[2]
            blocksize = int(fields[4])
            return (addr, blocksize)
    except:
        pass

# filters the blocks.csv
blocks = sc.textFile('/data/ethereum/blocks').filter(line_check)
# uses function to extract the address and blocksize
address_blocksize = blocks.map(addr_blocksize)
# aggregates the results for each address
aggregates = address_blocksize.reduceByKey(add)
# selects the top 10 results
top10 = aggregates.takeOrdered(10, key=lambda x: -x[1])

# prints results
print('        Most active miners                   Size of blocks')
for miner in top10:
    print('{0} - {1}'.format(miner[0],miner[1]))
