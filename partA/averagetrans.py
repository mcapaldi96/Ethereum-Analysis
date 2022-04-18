# import libraries
from mrjob.job import MRJob
import time

class PartA2(MRJob):

    def mapper(self, _, line):
        try:
            # splits the columns into fields
            fields = line.split(',')
            # collects the timestamp values
            timestamp = int(fields[6])
            # each transaction's value
            transaction_value = float(fields[3]) / 1000000000000000000
            # converts from unix-timestamp to year-month format
            year_month = time.strftime('%Y-%m', time.gmtime(timestamp))
            # yields each year-month with a count and the transaction value
            yield (year_month,{'count': 1,'transaction_value':transaction_value})
        except:
            pass

    def combiner(self, key, value):
        # starts a counter for total values and count
        total_value = 0.0
        count = 0
        # iterates through every transaction value and updates count and total value tallies
        for val in value:
            total_value += val['transaction_value']
            count += val['count']
        # returns result of the accumulated count and total transaction_value for each year-month
        result = {'count':count,'transaction_value':total_value}
        yield (key, result)

    def reducer(self, key, value):
        # starts a counter for total values and count
        total_value = 0.0
        count = 0
        # iterates through every transaction value and updates count and total value tallies
        for val in value:
            total_value += val['transaction_value']
            count += val['count']
        # computes the average value
        average_value = total_value / count
        # yields the average value for every year-month key
        yield (key, average_value)

if __name__ == '__main__':
    PartA2.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartA2.run()
