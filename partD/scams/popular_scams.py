import pyspark
from operator import add
import time

sc = pyspark.SparkContext()

def line_check(line):
    try:
        # splits dataset on ','
        fields = line.split(',')
        # first if checks the transactions dataset
        if len(fields) == 7:
            # address field
            str(fields[2])
            # checks the value field is >0
            if int(fields[3]) == 0:
                return False
        # second if checks the scams dataset
        elif len(fields) == 8:
            #  address field
            str(fields[6])
            # type of scam
            str(fields[4])
            # scam status
            str(fields[7])
        else:
            return False
        return True
    except:
        return False


# function collects all the needed information for the transaction dataset
def transaction_info(line):
    try:
        # splits dataset
        fields = line.split(',')
        # collects to address
        addr = fields[2]
        # collects transaction value (wei)
        value = int(fields[3])
        # records the value and 1 so we can keep track of the number of scams
        value = (value, 1 )
        # returns the address and value as key,value pair
        return (addr, value)
    except:
        pass

# function collects all the needed information for the scams dataset
def scams_info(line):
    try:
        # splits dataset
        fields = line.split(',')
        # collects address
        addr = fields[6]
        # collects type of scam
        scam_type = str(fields[4])
        # collects scam status
        status = str(fields[7])
        # places scam_type and scam status into a tuple to be used as a value for (key,value) pair
        value = (scam_type, status)
        # returns key,value pair
        return (addr, value)
    except:
        pass

# both transactions and scams datasets are checked using line_check function
transactions = sc.textFile('/data/ethereum/transactions').filter(line_check)
scams = sc.textFile('scams.csv').filter(line_check)

# extracting information from transaction and scams dataset
trans_mapped = transactions.map(transaction_info)
scams_mapped = scams.map(scams_info)
# joins the transaction and scams information together on the addresses
trans_scams_join = trans_mapped.join(scams_mapped)
# extracts the scam type, status, value and count
extracted_info = trans_scams_join.map(lambda x: ((x[1][1]), (x[1][0])))
# aggregates the values for each address
aggregates = extracted_info.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
# takes the results and extracts the type of scam, status and volume
results = aggregates.map(lambda x: '{} - {} - {} - {}'.format(x[0][0], x[0][1], float(x[1][0]), x[1][1]))

# saves results as a text file
results.saveAsTextFile('popular_scams')
