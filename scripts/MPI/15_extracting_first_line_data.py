#! /opt/sharcnet/python/2.7.5/intel/bin/python

# sqsub -r 5m -f xeon -q mpi --mpp 2GB -o first_line_extract -n 2 python 4_SCRIPTS/MPI/15_extracting_first_line_data.py 0

# for ((i=0; i<=3;i++)); do echo `sqsub -r 5m -f xeon -q mpi --mpp 1GB -o first_line_extract -n 2 python 4_SCRIPTS/MPI/15_extracting_first_line_data.py $i`; done

# for ((i=0; i<=72;i++)); do echo `sqsub -r 30m -f xeon -q mpi --mpp 1GB -o first_line_extract -n 8 python 4_SCRIPTS/MPI/15_extracting_first_line_data.py $i`; done


# ##################################################
# This scripts extracts the first line of metadata
# and stores it in a TXT file
# ##################################################


from mpi4py import MPI
import sys
import os
import tarfile
import gzip
from optparse import OptionParser
import tempfile
import GVM_classes
import Features


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
print "SIZE:{0}, RANK:{1}".format(size, rank)

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')  # ComputeCanada
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

    # LOCAL FILES
    # input_dir = '/Users/gabriel/Dropbox/1_PHD_VC/9_SHARED_DATA/IN'  # LOCAL GVM
    # output_file = '/Users/gabriel/Dropbox/1_PHD_VC/9_SHARED_DATA/out.txt'
    # input_dir = '/Users/gabriel/9_TEST/IN_MEDIUM' # LOCAL GVM
    # input_dir = '/Users/gabriel/Dropbox/9_TEST/1_TARS/' # LOCAL STARR

    # COMPUTE CANADA FILES
    # input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS/IN' # simplest case
    # input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/IN' #8 TAR files
    input_dir = '/work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' #general case
    output_file = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS/OUT/out'

    # print '\nINPUT DIR:{0}'.format(input_dir)

    # ##########################################################
    # Extracts all files within a TAR to TEMP_FOLDER
    # TODO: Extract only in one node? (which one) and bcast to
    # the other ones
    # ##########################################################

    file_list = []  # List of all files in input_dir
    for root, subFolders, files in os.walk(input_dir):
        for f in files:
            if f == '.DS_Store':
                continue
            file_list.append('/'.join([root, f]))

    # print 'FILE_LIST:{0}'.format(file_list)
    list_factor = size * int(factor) + rank  # unique value for each parallel instance of the code
    # print "LIST FACTOR:{0}".format(list_factor)
    filename = file_list[list_factor]
    # print 'FILENAME:{0}'.format(filename)
    tar_object = tarfile.open('/'.join([filename]))
    tar_object.extractall(TEMP_FOLDER)

    # print "SIZE:{0}, TAR FILENAME: {1}".format(size * int(factor) + rank, filename)
    # ################################
    # Saving
    # ################################

    # Iterate over all files in a TAR
    file_list = GVM_classes.folder_iterator(TEMP_FOLDER)
    # print 'FILELIST:{0}'.format(file_list)
    for file_in_tar in file_list:
        if file_in_tar.split('/')[-1].startswith('.'):
            continue
        # print 'FILE IN TAR: {0}'.format(file_in_tar)
        gzipfile = gzip.open(file_in_tar)
        metadata = gzipfile.readline()

        # Dumping filtered data to GZIP
        with open(output_file + '_' + str(list_factor) + '.txt', 'ab') as f:
            f.write(metadata)
