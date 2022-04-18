from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class fork(MRJob):

    def mapper(self,_,line):
        try:
            # splits data into fields
            fields = line.split(',')
            # records address and transaction gas price and timestamp
            address = str(fields[2])
            value = float(fields[5])
            date = time.gmtime(float(fields[6]))
            if len(fields) == 7:
                # select a fork date
                if (date.tm_year == 2016 and date.tm_mon == 7 and (date.tm_mday == 20)):
                    # yields key,value pair where
                    # key = address, value = gas price value and count of 1
                    yield (address, (1, value))
        except:
            pass

    def reducer(self,key,value):
        # for each address the total gas value and no. of transactions is calculated
        count = 0
        total = 0
        for v in value:
            count += v[0]
            value = v[1]
        yield (key, value)

    def mapper_2(self, key, value):
        yield None, (key, value)

    def reducer_2(self,_,keys):
        # sorts the results and yields the top 10 address after the fork
        values_sorted = sorted(keys, reverse=True, key = lambda x: x[1])
        for value in values_sorted[:10]:
            yield value[0], value[1]

    # steps function orders the use of the mappers and reducers and returns output
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(mapper=self.mapper_2, reducer=self.reducer_2)]


if __name__=='__main__':
    fork.run()
