#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 20m -f xeon -q mpi --mpp 2GB -o billboard_to_h5 -n 2 python ./4_SCRIPTS/MPI/6_checking_double_usernames.py 0 
# sqsub -r 40m -f xeon -q mpi --mpp 2GB -o billboard_to_h5 -n 2 python ./4_SCRIPTS/MPI/6_checking_double_usernames.py 0 

# ################################################################################
# Script for finding duplicate usernames in lastfm user
# ################################################################################

from mpi4py import MPI 
import sys, os
import tarfile, gzip
import GVM_classes
import Features
import tempfile
import h5py
from optparse import OptionParser


rank = MPI.COMM_WORLD.Get_rank() 
size = MPI.COMM_WORLD.Get_size() 
name = MPI.Get_processor_name()
# print "\nHelloworld! I am process {0} of {1} on {2}".format(rank, size, name)


TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
SQ_JOBID = os.environ["SQ_JOBID"]


if __name__ == "__main__":
	usage = "usage: %prog [options] factor"
	opts = OptionParser(usage = usage)
	opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# Factor to overcome the 256 CPUs limitation.
	# Each run must have a different factor
	# E.g., to run the 583 TAR files, it would be 
	# possible to do 256, 256, and 71. However
	# it is more balanced to do 192, 192, and 199; as in 
	# sqsub -r 12m -q mpi -o test_mpi -n 192 python ./4_SCRIPTS/MPI/helloworld.py [0-2]

	factor = args[0]

	log_file = open('.checking_double_usernames_errors.log', 'a')


	# Init parameteres for where userfiles are
	# Creates TAR object and extracts its members to TMP folder
	input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB'
	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			file_list.append('/'.join([root,f]))
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	# print rank, file_list[size * int(factor) + rank]

	# ##########################################################
	# Iterate over all files in a TAR, searching for all songs
	# ##########################################################

	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER):
		try:
			user_features = Features.ListeningFeatures(file_in_tar)	# Initializes object for feature extraction 
			
			lfid = user_features.lfid
			username = user_features.username
			print lfid,'\t', username,'\t', file_list[size * int(factor) + rank]
		except Exception, e:
			# log_file.write(str(e) + '\n')
			log_file.write(lfid + '\t' + username + '\t' + str(file_list[size * int(factor) + rank]) + '\t' + str(e) + '\n')



	# h.close()
	MPI.Finalize()


