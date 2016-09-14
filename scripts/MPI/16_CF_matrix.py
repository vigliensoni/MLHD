from optparse import OptionParser
from pandas import *
import h5py
import numpy as np
from Features import *
from GVM_classes import *


def dfWriter(inputFolder):
    """
    Pandas-based method for merging a list of dicts
    Taken from http://pandas.pydata.org/pandas-docs/dev/dsintro.html#from-a-list-of-dicts
    """
    files = GVM_classes.folder_iterator(inputFolder)
    outData = []
    indexData = []
    for i, f in enumerate(files):
        print f
        metaFeat = MetadataFeatures(f)
        lfid = int(metaFeat.lfid)
        listFeat = ListeningFeatures(f)
        mbidFreq = listFeat.artist_mbid_frequencies(empty_mbids=False)
        outData.append(mbidFreq)
        indexData.append(lfid)

    df = pandas.DataFrame(outData, indexData)
    df.fillna(0, inplace=True, downcast='infer')
    # df = df.astype(int)
    return df


def pickledDataframe2hdf5(dataframe, dataframeName, outputFilename):
    """
    Converts a pickled Pandas dataframe to HDF5
    """
    d = pandas.HDFStore(outputFilename)
    d[dataframeName] = dataframe
    d.close()





if __name__ == "__main__":
    """
    This file should be able of making a 2*2 matrix with data of users and items.
    It currently extracts individual matrixes, but it does not merges them
    """
    usage = "usage: %prog [options] inputFolder outputFile"
    opts = OptionParser(usage=usage)
    options, args = opts.parse_args()

    inputFolder = args[0]
    outputFilename = args[1]

    df = dfWriter(inputFolder)
    df.to_pickle(outputFilename)
    print df

# Merge datasets
# dfm = pd.merge(df1, df2, how='outer')

# 