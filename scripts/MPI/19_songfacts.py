#! /opt/sharcnet/python/2.7.5/intel/bin/python

#                                 sqsub -r 30m -f xeon -q mpi --mpp 2GB -o songfact -n 2 python 4_SCRIPTS/MPI/19_songfacts.py 0

# for ((i=0; i<=3;i++)); do echo `sqsub -r 15m -f xeon -q mpi --mpp 2GB -o songfact -n 2 python 4_SCRIPTS/MPI/19_songfacts.py $i`; done

# for ((i=0; i<=72;i++)); do echo `sqsub -r 30m -f xeon -q mpi --mpp 2GB -o songfact -n 8 python 4_SCRIPTS/MPI/19_songfacts.py $i`; done

# for ((i=7269942; i<=7270026;i++)); do echo `sqkill $i`; done

# ##################################################
# This scripts should
# 1. Open each TAR file
# 2. extract listening frequencies for all users
# 3. 
# ##################################################


from mpi4py import MPI
import os
import shutil
import tarfile
from optparse import OptionParser
import tempfile
import GVM_classes
import Features
# import SEM


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
# print "SIZE:{0}, RANK:{1}".format(size, rank)

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens')  # ComputeCanada
# print 'TEMP_FOLDER: ', TEMP_FOLDER
# TEMP_FOLDER = tempfile.mkdtemp(dir='/Users/gabriel/scratch/') # Local

# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
try:
    SQ_JOBID = os.environ["SQ_JOBID"]
except:
    SQ_JOBID = '_STARR'

if __name__ == "__main__":
    usage = "usage: %prog [options] factor"
    opts = OptionParser(usage=usage)
    # opts.add_option('-f', '--hdf5', dest='h5')
    options, args = opts.parse_args()

    factor = args[0]
    # abdata = args[1] #AcousticBrainz data

    # LOCAL FILES
    # input_dir = '/Users/gabriel/Dropbox/1_PHD_VC/9_SHARED_DATA/IN'  # LOCAL GVM
    # output_file = '/Users/gabriel/Dropbox/1_PHD_VC/9_SHARED_DATA/out.txt'
    # input_dir = '/Users/gabriel/9_TEST/IN_MEDIUM' # LOCAL GVM
    # input_dir = '/Users/gabriel/Dropbox/9_TEST/1_TARS/' # LOCAL STARR

    # COMPUTE CANADA FILES
    # input_dir = '/work/vigliens/GV/5_TEST'  # simplest case
    # output_folder = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS/OUT/'

    # input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/IN' #8 TAR files
    # output_folder = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/OUT/'

    input_dir = '/work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' #general case
    output_folder = '/scratch/vigliens/out/'

    # print '\nINPUT DIR:{0}'.format(input_dir)

    # ##########################################################
    # Extracts all files within a TAR to TEMP_FOLDER
    # TODO: Extract only in one node? (which one) and bcast to
    # the other ones
    # ##########################################################

    file_list = GVM_classes.folder_iterator(input_dir)  # List of all files in input_dir
    list_factor = size * int(factor) + rank  # unique value for each parallel instance of the code
    filepath = file_list[list_factor]
    tar_object = tarfile.open('/'.join([filepath]))
    tar_object.extractall(TEMP_FOLDER)

    print "SIZE:{0}, TAR FILENAME: {1}".format(size * int(factor) + rank, filepath)

    # loading list with required MBIDs
    target_mbids_file = open('/home/vigliens/Documents/2_CODE/songfacts_mbids.txt')
    target_mbids = target_mbids_file.readlines()
    target_mbids = [t.strip() for t in target_mbids]

    # ################################
    # Saving
    # ################################

    def dictionary2list(usid, dictionary):
        """
        Converts dictionary to list, prepending user id
        """
        return [(int(usid), i[0], i[1]) for i in dictionary.iteritems()]

    file_list = GVM_classes.folder_iterator(TEMP_FOLDER)

    for file_in_tar in file_list:
        if file_in_tar.split('/')[-1].startswith('.'):
            continue
        lf = Features.ListeningFeatures(file_in_tar)
        mbid_freq = lf.track_mbid_frequencies(empty_mbids=False)
        source_mbids = mbid_freq.keys()
        # intersection = set(target_mbids) & set(source_mbids)
        intersection = set.intersection(set(target_mbids), set(source_mbids))

        if len(intersection) >= 400:
            # do write results
            outputfilename = output_folder + GVM_classes.filenameparser(file_in_tar, 'basename').split('.')[0] + '.csv'
            frequencies = dictionary2list(lf.lfid, mbid_freq)
            GVM_classes.tuple2tsv(frequencies, outputfilename)

    shutil.rmtree(TEMP_FOLDER)
