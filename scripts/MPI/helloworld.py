#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 4m -q mpi -o test_mpi -N 1 -n 8 ./4_SCRIPTS/MPI/helloworld.py


from mpi4py import MPI 
import sys
import os
import tarfile
import gzip
import GVM_classes
import Features 
import tempfile
from optparse import OptionParser


rank = MPI.COMM_WORLD.Get_rank() 
size = MPI.COMM_WORLD.Get_size() 
name = MPI.Get_processor_name()
# print "\nHelloworld! I am process {0} of {1} on {2}".format(rank, size, name)

rank_lead_zero = "%02d" % (rank + 1,)
TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
# JOBID = os.environ["SQ_JOBID"]


def gzip_tar_member(member):
	"""
	Read zipped file and return a list of lines
	Zip File -> List
	"""
	gf = gzip.GzipFile(member)
	return gf.readlines()


if __name__ == "__main__":
	usage = "usage: %prog [options] factor"
	opts = OptionParser(usage = usage)
	options, args = opts.parse_args()

	# Factor to overcome the 256 CPUs limitation.
	# Each run must have a different factor
	# E.g., to run the 583 TAR files, it would be 
	# possible to do 256, 256, and 71. However
	# it is more balanced to do 192, 192, and 199; as in 
	# sqsub -r 12m -q mpi -o test_mpi -n 192 python ./4_SCRIPTS/MPI/helloworld.py [0-2]

	factor = args[0]
	

	# # Init parameters

	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS/'
	# file_list = os.listdir(input_dir)
	# # print rank, file_list[rank]

	input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB'
	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			file_list.append('/'.join([root,f]))


	# Creates TAR object and extracts its members to TMP folder
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	# Iterates over all members and prints first line
	list_to_print = []
	for member in GVM_classes.folder_iterator(TEMP_FOLDER):
		# print member
		l = gzip_tar_member(member)
		list_to_print.append(l[0].strip())

	for to_print in list_to_print:
		print to_print





	MPI.Finalize()



