import sys
import os
import pickle
import string
from multiprocessing import Pool
import copyreg
import types
from nltk.tokenize import word_tokenize

# A class for the MapReduce framwork
class MapReduce(object):

    def __init__(self, m, r, path):
        self.maptask = m # number of mapper?
        self.reducetask = r
        self.path = path
        self.Split(path)

    # Split() splits the input file into #maptask temporary files, each with a keyvalue, respecting word boundaries
    # The keyvalue is the byte offset in the input file
    def Split(self, path):
        size = os.stat(self.path).st_size
        chunk = size / self.maptask #unit is in bytes
        chunk += 1  #why need Add one bytes?
        f = open(self.path, "r",encoding="UTF-8",errors='ignore')
        buffer = f.read() #read all words from text file
        f.close()
        f = open("#split-%s-%s" % (self.path, 0), "w+",encoding="UTF-8") #create tempory file and can be write byte
        f.write(str(0) + "\n")
        i = 0
        m = 1
        for c in buffer: #keep writing new chars to the temporay file
            f.write(c)
            i += 1 #number of chars (bytes)

            # close the curr temp file if end of whatsapec and
            # the total item written so far is larger than bytes amount of each mapper
            if (c in string.whitespace) and (i > chunk * m):
                f.close()
                m += 1
                f = open("#split-%s-%s" % (self.path, m-1), "w+",encoding="UTF-8")
                f.write(str(i) + "\n")
        f.close()

    # Maps value into a list of (key, value) pairs
    # To be defined by user of class
    def Map(self, keyvalue, value):
        pass

    # Determines the default reduce task that will receive (key, value)
    # It uses hash functions to map intermediate (key, value) pairs to specific reduce task
    # User of class can overwrite the default.
    def Partition(self, item):
        return hash(item[0]) % self.reducetask

    # Reduces all pairs for one key [(key, value), ...])
    # To be defined by user of class
    def Reduce(self, key, keyvalues):
        pass

    # Optionally merge all reduce partitions into a single output file
    # A better implementation would do a merge sort of the reduce partitions,
    # since each partition has been sorted by key
    def Merge(self):
        out = {}
        for r in range(0, self.reducetask):
            f = open("#reduce-%s-%d" % (self.path, r), "rb")
            partition = dict(pickle.load(f))
            out = { k: out.get(k, 0) + partition.get(k, 0) for k in out.keys() | partition.keys() } #get keys in either out or partition
            f.close()
            os.unlink("#reduce-%s-%d" % (self.path, r))
        out = sorted(out.items(), key=lambda pair: pair[0])
        return out

    # Load a mapper's split and apply Map to it
    def doMap(self, i):
        f = open("#split-%s-%s" % (self.path, i), "r",encoding="UTF-8",errors='ignore')
        keyvalue = f.readline() # read the first line only
        value = f.read() # read all individual characters
        f.close()
        os.unlink("#split-%s-%s" % (self.path, i)) #delete the tempory file
        keyvaluelist = self.Map(keyvalue, value)
        for r in range(0, self.reducetask):
            f = open("#map-%s-%s-%d" % (self.path, i, r), "wb+")
            itemlist = [item for item in keyvaluelist if self.Partition(item) == r] #only hash to the correpsonding reducer
            pickle.dump(itemlist, f) # write the itemlist to file using pickle
            f.close()
        return [(i, r) for r in range(0, self.reducetask)]

    # Get reduce regions from maptasks, sort by key, and apply Reduce for each key
    def doReduce(self, i):
        keys = {}
        out = []
        # this will only update the key dict
        for m in range(0, self.maptask):
            f = open("#map-%s-%s-%d" % (self.path, m, i), "rb")
            itemlist = pickle.load(f)
            for item in itemlist:
                if item[0] in keys:
                    keys[item[0]].append(item) #list of tuples
                else:
                    keys[item[0]] = [item] # list of tuples
            f.close()
            os.unlink("#map-%s-%s-%d" % (self.path, m, i))
        for k in sorted(keys.keys()):
            out.append(self.Reduce(k, keys[k]))
        f = open("#reduce-%s-%d" % (self.path, i), "wb+")
        # out is a list of tuples
        pickle.dump(out, f)
        f.close()
        return i

    # run() creates a pool of processes and invokes maptask Map tasks
    # Each Map task applies Map() to 1/maptask-th of the input file, and partitions its output in reducetask regions, for a total of maptask x reducetask Reduce regions (each stored in a separate file). 
    # run() then creates reducetask Reduce tasks
    # Each Reduce task reads maptask regions, sorts the keys, and applies Reduce to each key, producing one output file. The reducetask output files can be merged by invoking Merge().
    def run(self):
        pool = Pool(processes=max(self.maptask, self.reducetask),)
        regions = pool.map(self.doMap, range(0, self.maptask))
        partitions = pool.map(self.doReduce, range(0, self.reducetask))

# Python doesn't pickle method instance by default, so here you go:
def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
    return _unpickle_method, (func_name, obj, cls)

def _unpickle_method(func_name, obj, cls):
    for cls in cls.mro():
        try:
            func = cls.__dict__[func_name]
        except KeyError:
            pass
        else:
            break
    return func.__get__(obj, cls)

copyreg.pickle(types.MethodType, _pickle_method, _unpickle_method)


# You could build a WordCount class to take charge of the specific job
class WordCount(MapReduce):

    def __init__(self, maptask, reducetask, path):
        super().__init__(maptask, reducetask, path)

    # Produce a (key, value) pair for each title word in value
    def Map(self, keyvalue, value):
        tokens=word_tokenize(value) # used standford NLPTK to tokenize the word
        #print(type(tokens[0]))
        itemlist=[]
        # prepare a list of puntuations or non-words signs after tokenization to avoid
        punctuations=set([*string.punctuation]+["``","''",'""','’','“','”',"—","..."])
        # prepare a list of stop words to extract meaningful frequent words
        with open("./stop_words.txt",'r',encoding="UTF-8") as f:
            stop_words=f.read()
            stop_words=set(stop_words.split(','))

        for token in tokens:
            token=token.lower()
            if token in (stop_words|punctuations): #avoid adding to item list if its punctuation or a stop words
                continue
            itemlist.append((token,1))

        return itemlist


    # Reduce [(key,value), ...]) 
    def Reduce(self, key, keyvalues):
        return (key,len(keyvalues))


if __name__ == '__main__':
    # Read files using the command "python3 mapreduce.py file.txt"
    # if (len(sys.argv) != 2):
    #     print("Program requires path  to filefor reading!")
    #     sys.exit(1)

    file_path="hp.txt"

    # Create a WordCount MapReduce program
    wc = WordCount(2, 2, file_path) # sys.argv[1]
    # Run it
    wc.run()
    # Merge out of Reduce tasks:
    out = wc.Merge()
    # Sort by word count:
    out = sorted(out, key=lambda pair: pair[1], reverse=True)
    # Print top 20:
    print("WordCount:")
    for pair in out[0:20]:
        print(pair[0], pair[1])
