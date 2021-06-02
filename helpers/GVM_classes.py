import numpy as np
import pandas as pd
import os
import time
import csv
import pickle as cPickle
import re
import collections
import shutil
from dateutil.parser import parse
from unidecode import unidecode
import glob
import itertools
import codecs
# import cStringIO
import io


# from numpy import NaN, Inf, arange, isscalar, array, argmax


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = io.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def UnicodeDictReader(str_data, encoding, **kwargs):
    csv_reader = csv.DictReader(str_data, **kwargs)
    # Decode the keys once
    keymap = dict((k, k.decode(encoding)) for k in csv_reader.fieldnames)
    for row in csv_reader:
        yield dict((keymap[k], v.decode(encoding)) for k, v in row.iteritems())


def remove_accents(string):
    """
    Removes special characters such as accents and returns plain text.
    The string has to be unicode.
    """
    return unidecode(unicode(string, 'utf_8'))

class SolrInteraction():
    """
    Class for handling all interaction with Solr instance
    """


class StatisticalMethods:
    """
    Class for performing statistic analysis on standard arrays
    """

    def __init__(self):
        return

    def normalize_array(self, input_array, new_min=0, new_max=1):
        """
        Returns an array with scaled values for an array between x and y, using the form

        f(x) = (b-a)(x-min)/(max-min) + a
        """
        input_array = np.array(input_array)
        min_value = input_array.min()
        max_value = input_array.max()
        diff = max_value - min_value  # check if denominator is 0
        if diff is not 0:
            norm_data = [(((new_max - new_min) * (x - min_value) / float(max_value - min_value)) + new_min) for x in input_array]
        else:
            norm_data = [0 for x in input_array]
        return norm_data


    def peakdet(self, v, delta, minimum_y_threshold=0, minimum_x_threshold=1, x=None):
        """
        Look for maximum and minimum peaks and returns two arrays

        Based on MATLAB script at http://billauer.co.il/peakdet.html

        function [maxtab, mintab]=peakdet(v, delta, x)
        %        [MAXTAB, MINTAB] = PEAKDET(V, DELTA) finds the local
        %        maxima and minima ("peaks") in the vector V.
        %
        %        MAXTAB and MINTAB consists of two columns. Column 1
        %        contains indices in V, and column 2 the found values.
        %
        %        With [MAXTAB, MINTAB] = PEAKDET(V, DELTA, X) the indices
        %        in MAXTAB and MINTAB are replaced with the corresponding
        %        X-values.
        %
        %        A point is considered a maximum peak if it has the maximal
        %        value, and was preceded (to the left) by a value lower by
        %        DELTA.
        %
        %        MINIMUM_Y_THRESHOLD is the threshold for minimum maximum
        %           values
        %        MINIMUM_X_THRESHOLD: minimum horizontal difference between 2
        %           maximum candidates

        Example usage:
        
        import math
        series = [100*math.sin(x) for x in xrange(1000)]
        maxtab, mintab = sm.peakdet(series, delta = 1)
        plt.plot(series)
        plt.scatter(array(maxtab)[:,0], array(maxtab)[:,1], color='blue')
        plt.show()

        """

        maxtab = []
        mintab = []
           
        if x is None:
            x = np.arange(len(v))
        
        v = np.array(v)
        
        if len(v) != len(x):
            sys.exit('Input vectors v and x must have same length')
        
        if not np.isscalar(delta):
            sys.exit('Input argument delta must be a scalar')
        
        if delta <= 0:
            sys.exit('Input argument delta must be positive')
        
        mn, mx = np.Inf, - np.Inf
        mnpos, mxpos = np.NaN, np.NaN
        
        lookformax = True
        
        for i in np.arange(len(v)):
            this = v[i]
            if this > mx:
                mx = this
                mxpos = x[i]
            if this < mn:
                mn = this
                mnpos = x[i]
            
            if lookformax:
                if this < mx-delta and this >= minimum_y_threshold:
            
                    maxtab.append((mxpos, mx))
                    mn = this
                    mnpos = x[i]
                    lookformax = False
            else:
                if this > mn+delta:
                    mintab.append((mnpos, mn))
                    mx = this
                    mxpos = x[i]
                    lookformax = True
     
        # check for close max candidates, choose only one
        for i in range(len(maxtab)-1, 0, -1):
            if maxtab[i][0]-maxtab[i-1][0] < minimum_x_threshold: # minimum x threshold
                if maxtab[i][1] >= maxtab[i-1][1]:
                    del maxtab[i-1]
                else:
                    del maxtab[i]

        return np.array(maxtab), np.array(mintab)

# #####################
# TIME
# #####################


def now():
    """
    ;; -> TUPLE ( , , )
    ;; Return localtime in the form (HH,MM,SS)
    """
    now = time.localtime()
    return (now.tm_hour, now.tm_min, now.tm_sec)


def date_to_uts(date):
    """
    Converts date in the form YEAR-MN-DY to UTS
    String -> Integer
    """
    epoch = '1970-01-01'
    date_utc = parse(date)
    return (parse(date).toordinal() - parse(epoch).toordinal()) * 24*60*60


def timestring():
    """
    Create string from local time.

    The resulting is a string with padded zeros. It follows the form

    YYYYMMDDHHMM
    """
    year = str(time.localtime()[0])
    month = str(time.localtime()[1]).zfill(2)
    day = str(time.localtime()[2]).zfill(2)
    hour = str(time.localtime()[3]).zfill(2)
    minute = str(time.localtime()[4]).zfill(2)

    return ''.join([year, month, day, hour, minute])




# #####################
# OS
# #####################

class FileMetadata():
    """Return metadata from a file."""

    def birthdate(self, filepath):
        """Return creation date for a file in UNIX timestamp."""
        bd = int(os.stat(filepath).st_birthtime)
        return bd




def folder_iterator(input_folder, returnFullpath=True):
    """
    ;; String -> List
    ;; Receive a string with a input_folder string and return
    ;;  a list with all full file paths on that folder
    """

    filepath_list = []
    for dirpath, dirnames, filenames in os.walk(input_folder):
        # COUNTER = len(filenames)
        for f in filenames[:]:
            if f == '.DS_Store' or f == '.AppleDouble' or f == '.Parent':
                continue
            if returnFullpath is True:
                full_path = os.path.join(dirpath, f)
            else:
                full_path = f
            filepath_list.append(full_path)
    return filepath_list

def ensure_dir(f):
    """
    Check if a directory exists and create it if necessary
    """
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

def file_opener(filepath):
    """
    ;; String -> List
    ;; Receive a filepath and return a list of rows
    """

    input_file = open(filepath, 'rb')
    lines = input_file.readlines()
    input_file.close()
    return lines

def uuidRegexCompiler():
    """
    UUID compiler

    Return regex compiler
    """
    re_uuid = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', re.I)
    return re_uuid

def uuidSearcher(string):
    """
    Finds and return only the UUID embedded in a string

    input:
    '/foo/bar/uuid/000009a8-34f1-4c58-a8de-1d99809cd626-1.json'

    output:
    '000009a8-34f1-4c58-a8de-1d99809cd626'
    """
    re_uuid = uuidRegexCompiler()
    if re.search(re_uuid, string) is not None:
        uuid = re.search(re_uuid, string).group()
    # uuid = [re.search(re_uuid, s).group() for s in array if re.search(re_uuid, s) is not None]
    return uuid


def filenameparser(string, type=''):
    """
    Parses a filepath in several ways
    1. type = '' returns [filename, fileextension]
    2. type = 'basename' returns basename
    """
    if type == '':
        return os.path.splitext(string)
    elif type == 'basename':
        return os.path.basename(string)
# #####################
# FOLDER STRUCTURES
# #####################


def folderstructure_generator(filesFolder, outputFolder):
    """
    It takes as input a folder full of files, and copies all files within
    that folder into a new folder with the structure
    filename:           ./mbid.json
    new file structure: ./m/mb/mbid.json
    It checks if the folder structure exists, otherwise create it.
    String, String -> FolderStructure
    """
    allFiles = folder_iterator(filesFolder)
    # print allFiles
    for fullFilename in allFiles:
        # print fullFilename
        filename = fullFilename.split('/')[-1]
        print(filename)
        finalFolderpath = '/'.join([outputFolder, filename[0], filename[0:2]])
        finalFilepath = '/'.join([outputFolder, filename[0], filename[0:2], filename])
        if os.path.isdir(finalFolderpath) is False:
            os.makedirs(finalFolderpath)
        shutil.copy(fullFilename, finalFilepath)


def csvtodict(csvfilepath):
    r"""
    Convert csv file to dict.

    CSV should come in the form
    key1\tvalue1
    key2\tvalue2
    ...
    keyn\tvaluen

    """
    with open(csvfilepath, mode='r') as infile:
        lines = infile.readlines()
        mydict = {line.split()[0]: line.split()[1] for line in lines}
    return mydict


def filenameChecker(inputFolder, mbid):
    """
    Check if filename in the form m/mb/mbid??.json exists.

    Returns boolean
    """
    fi = mbid[:1]
    se = mbid[:2]

    location = glob.glob('/'.join([inputFolder, fi, se, mbid]) + '??.json')
    if len(location) != 0:  # if a file exists
        return os.path.isfile(location[0])  # True
    else:
        return False


# #####################
# DICTIONARIES
# #####################


def dict_merger_adder(d1, d2, method='merging'):
    """
    Merge two dictionaries adding the values of common keys, as in:
    d1 = {'a': 1, 'b': 1}
    d2 = {'b': 2, 'c': 1}

    If 'merging' as method:
    dn = {'a': 1, 'b': 3, 'c': 1}

    If 'intersection' as method:
    dn = {b': 3}

    Taken from http://stackoverflow.com/questions/10461531/merge-and-sum-of-two-dictionaries

    Dict, Dict -> Dict
    """

    if method == 'intersection':
        dn = {k: d1.get(k, 0) + d2.get(k, 0) for k in set(d1) & set(d2)}
    else:
        dn = {k: d1.get(k, 0) + d2.get(k, 0) for k in set(d1) | set(d2)}
    return dn


def group_by_key(lista):
    """
    Group the values by key
    returns the unique keys, their corresponding per-key sum, and the keycounts
    Receives a np.array for keys and values
    Returns np.arrays for ordered keys and their total count

    Adapted from http://stackoverflow.com/questions/7790611/average-duplicate-values-from-two-paired-lists-in-python-using-numpy

    Assumes data in the form
    [(k1, v1), (k2, v2), ... , (kn, vn)]
    and returns it in the same form

    """
    # tup = np.array([('', e) for e in xrange(10)], dtype = npydt)

    # change data to the form [(k1, k2, ..., kn), (v1, v2, ..., vn)]
    # and upcast to numpy arrays
    key = np.asarray([x for x, y in lista])
    value = np.asarray([y for x, y in lista])
    # first, sort by key
    I = np.argsort(key)
    key = key[I]
    value = value[I]
    # the slicing points of the bins to sum over
    slices = np.concatenate(([0], np.where(key[:-1]!=key[1:])[0]+1))
    # first entry of each bin is a unique key
    unique_keys = key[slices]
    # sum over the slices specified by index
    per_key_sum = np.add.reduceat(value, slices)
    # number of counts per key is the difference of our slice points. 
    # cap off with number of keys for last bin
    # key_count = np.diff(np.append(slices, len(key)))
    return [(k, per_key_sum[i])  for i, k in enumerate(unique_keys)]
    # return unique_keys, per_key_sum#, key_count

# #####################
# CONVERSIONS
# #####################


def pickle_to_csv(input_file_path, output_file_path):
    """
    Pickle to csv converter
    """
    with(open(input_file_path, 'rb')) as handle:
        d = cPickle.load(handle)

    with(open(output_file_path, 'wb')) as handle_o:
        writer = csv.writer(handle_o)
        for key, value in d.items():
            writer.writerow([key, value])


def numpy_to_txt(input_file_path, output_file_path):
    """
    Numpy structured array to TXT

    [('x1', y1, z1), ('x2', y2, z2), . . . , ('xn', yn, zn)] -> File
    """
    numpy_array = np.load(input_file_path)
    with open(output_file_path, 'w') as f:
        for el in numpy_array[()]:
            line = ','.join([str(e) for e in el])
            f.write(line + '\n')


def dump_to_dict(dump_filepath):
    """
    Convert a numpy array in the form (('k1', v1), ('k2', v2), ... , ('kn', vn)) to a dictionary. It also deletes an empty key (''), and the dictionary is converted to a collection and is ordered by value
    """
    with open(dump_filepath, 'rb') as handle:
        f = cPickle.load(handle)
    d = {k: v for k, v in f}
    del f
    # do not consider empty MBID's
    if d.has_key(''):
        d.pop('', None)
    # return sorted ranking by value
    return collections.OrderedDict(sorted(d.items(), key=lambda t: t[1])) 


def dictionary2csv(dictionary, outfile):
    """
    Creates a CSV file from a dictionary with the form {'id1':1, 'id2': 2, ..., 'idn':n}.
    The CSV file is formed as:
    id1,1
    id2,2
    ...,...
    idn,n
    """
    with open(outfile, 'wb') as foo:
        w = csv.writer(foo)
        for row in dictionary.iteritems():
            w.writerow(row)


def dictionary2list(element, dictionary):
    """
    Converts dictionary to list, prepending element
    """
    return [(element, i[0], i[1]) for i in dictionary.iteritems()]


def tuple2tsv(lista, outfile):
    """
    Stores tuple any size to TSV file
    (2132, 'asdasd', 123)

    """
    with open(outfile, 'wb') as foo:
        w = csv.writer(foo, delimiter='\t')
        for row in lista:
            w.writerow(row)


def labelDataframe(dataframe, type='log', percentile_cuts=[0, 20, 40, 60, 80, 100]):
    """
    Maps frequencies of items to a 5-values Likert scale using a log function
    and labels the input dataframe with factor (a-la R's cut function)

    dataframe : Pandas dataframe to be processed
    percentile_cuts : cuts in a 0 to 100 scale
    type: 'log' for logarithmic, 'linear' for linear cut

    Returns a dataframe with the labeled values
    """
    max_frequency = max(dataframe['freq'])
    if type == 'log':
        # The log function, this one should be explained in the thesis
        cuts = np.round(np.exp(np.log(10) * np.linspace(np.log10(1), np.log10(max_frequency), len(percentile_cuts))))
        # print cuts
    else:
        cuts = percentile_cuts
    for i, c in enumerate(cuts[:-1]):  # Correct if there are repeated percentile bins
        if cuts[i + 1] - cuts[i] < 1:
            print('CORRECTED CUTS!')
            cuts[i + 1] = cuts[i] + 1
    labels = [str(x + 1) for x in range(len(percentile_cuts) - 1)]
    labeleddataframe = pd.cut(dataframe['freq'],
                              bins=cuts.astype(int),
                              include_lowest=True,
                              retbins=True,
                              labels=labels)
    return labeleddataframe


def frequencytoratingconverter(dictionary):
    """
    Converts frequencies in a dictionary to ratings.

    Return the ratings in a new column in a Pandas dataframe.
    """
    if len(dictionary) == 0:
        print('Empty dictionary')
        return pd.DataFrame(columns=['mbid', 'freq', 'likert'])
    histframe = pd.DataFrame(dictionary.items(), columns=['mbid', 'freq'])

    histframe[['mbid']] = histframe[['mbid']].astype('S32')
    # assigns likert-scale values fror frequencies
    data, bins = labelDataframe(histframe, type='log')
    histframe['likert'] = data.astype(int)
    return histframe


def reverses_orddict(ordereddict, list_no_to_order):
    """
    Reverses an OrderedDict by the element in list_no_to_order

    ranking = {'a':1, 'b':2, 'c':2, 'd':3}
    rev_ranking = reverses_orddict(ranking, 1)

    rev_ranking -> {'a':3, 'b':2, 'c':2, 'd':1}
    """

    dictionary_items = collections.OrderedDict(sorted(ordereddict.items(),
                                               key=lambda t: t[list_no_to_order])).items()
    dictionary_items.reverse()
    return collections.OrderedDict(dictionary_items)


def age_bracket(x):
    """
    Converts ages from [15,65] into age brackets (1,5). If age
    is out of that range, returns None

    Integer -> String

    """
    if 15 <= x < 25:
        return '1'
    elif 25 <= x < 35:
        return '2'
    elif 35 <= x < 45:
        return '3'
    elif 45 <= x < 55:
        return '4'
    elif 55 <= x < 65:
        return '5'
    else:
        return None


def permutations(lista=['A', 'B', 'C']):
    """
    Returns all permutation from a list

    lista = ['A', 'B', 'C']

    returns -> ['A', 'B', 'C', AB', 'AC', 'BC', 'ABC']
    """
    iterappend = []
    output = []
    for l in range(len(lista)):
        for e in itertools.combinations(lista, l+1):
            iterappend.append(e)
    for i, j in enumerate(iterappend):
        output.append([j for j in iterappend[i]])
    return output


def listprepender(lista, field='user'):
    """
    Prepend a first element to all elements in lista

    lista = ['A', 'B', 'C']
    field = ['X']

    returns -> [['X','A'], ['X', 'B'], ['X,'C']]
    """
    out = []
    for s in lista:
        s.insert(0, field)
        out.append(s)
    return out

# LUCENE



# Solr/Lucene special characters: + - ! ( ) { } [ ] ^ " ~ * ? : \
# There are also operators && and ||, but we're just going to escape
# the individual ampersand and pipe chars.
# Also, we're not going to escape backslashes!
# http://lucene.apache.org/java/2_9_1/queryparsersyntax.html#Escaping+Special+Characters
ESCAPE_CHARS_RE = re.compile(r'(?<!\\)(?P<char>[&|+\-!(){}[\]^"~*?:])')

def solr_escape(value):
    r"""Escape un-escaped special characters and return escaped value.

    >>> solr_escape(r'foo+') == r'foo\+'
    True
    >>> solr_escape(r'foo\+') == r'foo\+'
    True
    >>> solr_escape(r'foo\\+') == r'foo\\+'
    True
    """
    return ESCAPE_CHARS_RE.sub(r'\\\g<char>', value)
