# import libraries
from mrjob.job import MRJob
import time

class PartA1(MRJob):

    def mapper(self, _, line):
        try:
            # spliting the columns on ',' into fields
            fields = line.split(',')
            # extracting the timestamp
            timestamp = int(fields[6])
            # converting unix timestamp into Year-Month format
            year_month = time.strftime('%Y-%m', time.gmtime(timestamp))
            # yields each Year-Month and adds a count of 1 to it
            yield (year_month, 1)
        except:
            pass

    # combiner and reducer sums each transaction for each Year-Month key
    def combiner(self, key, value):
        yield (key, sum(value))

    def reducer(self, key, value):
        yield (key, sum(value))

# executing MapReduce
if __name__ == '__main__':
    #setting number of reducers to 5
    PartA1.JOBCONF = {'mapreduce.job.reduces': '5'}
    PartA1.run()

