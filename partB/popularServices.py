import pyspark
from operator import add
sc = pyspark.SparkContext()

def line_check_transactions(line):
    try:
        # split transaction data on ',' into fields
        fields = line.split(',')
        # checks there are the correct number of fields
        if len(fields) != 7:
            return False
        # fields[3] is the transaction value
        float(fields[3])
        return True
    except:
        return False

def line_check_contracts(line):
    try:
        # splits contract data on ','
        fields = line.split(',')
        # checks there are the correct number of fields
        if len(fields) != 5:
            return False
        return True
    except:
        return False
# collect contract data, filters the good rows using functions defined above
# then extracts the addresses by taking the 0th field
contract_data = sc.textFile('/data/ethereum/contracts')
contract_lines = contract_data.filter(line_check_contracts)
address = contract_lines.map(lambda l: (l.split(',')[0], 1))

# collect transaction data, filters the good rows using functions defined above
# then extracts the addresses and value of transaction in (address, value) format
transaction_data = sc.textFile('/data/ethereum/transactions')
transaction_lines = transaction_data.filter(line_check_transactions)
address_values = transaction_lines.map(lambda l: (l.split(',')[2], float(l.split(',')[3])))
# adds the values for each key 
aggregate_address_values = address_values.reduceByKey(add)
# results joins the addresses extracted from the contracts dataset
results = aggregate_address_values.join(address)
# orders the result and collects the top 10 entries
top10 = results.takeOrdered(10, key = lambda x: -x[1][0])

# prints each record
for rec in top10:
    print('{},{}'.format(rec[0], rec[1][0]))
