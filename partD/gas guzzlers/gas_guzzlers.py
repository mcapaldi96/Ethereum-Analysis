import pyspark
import time

sc = pyspark.SparkContext()

def line_check_transactions(line):
    try:
        fields = line.split(',')
        # checks tranaction dataset
        if len(fields) != 7:
            return False
        # ensures gas price and timestamps are float types
        float(fields[5]) # gas price
        float(fields[6]) # timestamp
        return True
    except:
        return False

def line_check_contracts(line):
    try:
        fields = line.split(',')
        # checks contract dataset
        if len(fields) != 5:
            return False
            # ensure block number is float type
        float(fields[3]) # block number
        return True
    except:
        return False

def line_check_blocks(line):
    try:
        fields = line.split(',')
        # checks blocks dataset
        if len(fields)!=9:
            return False
        # ensures the following variables are float types
        float(fields[0]) # block number
        float(fields[3]) # difficulty
        float(fields[7]) # timestamp
        return True
    except:
        return False

# reads in and filters transaction dataset
transactions = sc.textFile('/data/ethereum/transactions')
transaction_lines = transactions.filter(line_check_transactions)
# extracts the timestamp and gas price
time_gasprice = transaction_lines.map(lambda x: (float(x.split(',')[6]), float(x.split(',')[5])))
# converts timestamp into year-month format, and value of 1 is added to the price so we can calcuate an average later
time_gasprice = time_gasprice.map(lambda (t, pr): (time.strftime("%y-%m", time.gmtime(t)), (pr, 1)))
# for each year-month the gas prices are aggregated and then an average is calculated
price_time = time_gasprice.reduceByKey(lambda (pr1, n1), (pr2, n2): (pr1 + pr2, n1 + n2)).map(lambda x: (x[0], (x[1][0] / x[1][1])))
# results are sorted and saved as a text file
results = price_time.sortByKey(ascending=True)
results.saveAsTextFile('Average_Gas')

# reads in and filters contract dataset
contracts = sc.textFile('/data/ethereum/contracts')
contract_lines = contracts.filter(line_check_contracts)
# block number is extracted
block_no = contract_lines.map(lambda l: (l.split(',')[3], 1))

# reads in and filters blocks dataset
blocks = sc.textFile('/data/ethereum/blocks')
block_lines = blocks.filter(line_check_blocks)
# extracts block number (n), difficulty (d), gas used (g), and immediately converts raw timestamp to year-month (t) format
blocks_info = block_lines.map(lambda l: (l.split(',')[0], (int(l.split(',')[3]), int(l.split(',')[6]), time.strftime("%y-%m", time.gmtime(float(l.split(',')[7]))))))
# performs a join with block_no
results = blocks_info.join(block_no).map(lambda (id, ((d, g, t), n)): (t, ((d, g), n)))
# for each year-month results are aggregated and then results are saved
final = results.reduceByKey(lambda ((d1, g1), n1), ((d2, g2), n2): ((d1 + d2, g1 + g2), n1 + n2)).map(lambda x: (x[0], (float(x[1][0][0] / x[1][1]), x[1][0][1] / x[1][1]))).sortByKey(ascending=True)
final.saveAsTextFile('Difficulty_Time')
