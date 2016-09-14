#! /opt/sharcnet/python/2.7.5/intel/bin/python

# sqsub -r 30m -f xeon -q mpi --mpp 4GB -o .reduce_test -n 2 python 4_SCRIPTS/MPI/11_extracting_ranking_reduce.py 0

######################################################################
# STILL NOT WORKING, I'm getting a segfault error when comm.Gather()
######################################################################

from mpi4py import MPI 
import sys, os
import tarfile, gzip
from optparse import OptionParser
import tempfile
import GVM_classes, Features
import time
import numpy as np
import cPickle


comm = MPI.COMM_WORLD 
size = comm.Get_size() 
rank = comm.Get_rank() 

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
# SQ_JOBID = os.environ["SQ_JOBID"]

if __name__ == "__main__":
	usage = "usage: %prog [options] factor H5_FILES_ROOT"
	opts = OptionParser(usage = usage)
	# opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	factor = args[0]

	# input_dir = '/Users/gabriel/9_TEST/IN_SMALL' # LOCAL GVM
	input_dir = '/Users/gabriel/9_TEST/IN_MEDIUM' # LOCAL GVM
	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS' # simplest case
	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/IN' #8 TAR files
	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' #general case
	

	# ##########################################################
	# Extracts all files within a TAR to TEMP_FOLDER
	# TODO: Extract only in one node? (which one) and bcast to 
	# the other ones
	# ##########################################################

	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			if f == '.DS_Store':
				continue
			file_list.append('/'.join([root,f]))
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	print size * int(factor) + rank, file_list[size * int(factor) + rank]	


	# ##########################################################
	# Iterate over all files in a TAR, searching for all MBIDs
	# ##########################################################

	# NumPy structured datatype 
	# npydt = np.dtype({'mbid': ('S36', 0), 'pad' : ('|V4', 36), 'freq': (np.int64, 40)})
	npydt = np.dtype({'mbid': ('|S40', 0), 'freq': ('<i8', 40)})
	print 'NPYDT', npydt.itemsize

	mpidt = MPI.Datatype.Create_struct( 
		[40, 1], # block lengths 
		[0, 40], # displacements in bytes
		[MPI.CHAR, MPI.LONG], # MPI datatypes 
	).Commit() # don't forget to call Commit() after creation !!! 
	print 'MPI datatype size: ', mpidt.Get_size()


	t0 = time.time()
	cf = np.zeros((), dtype = npydt)
	# For each file in the folder extract features and add to list
	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER):
		# print 'RANK', rank, file_in_tar
		# Checking files starting with dot
		if file_in_tar.split('/')[-1].startswith('.'):
			continue
		# instance listening features
		lf = Features.ListeningFeatures(file_in_tar) 
		# extract key/value ranking of artist mbid frequencies
		# fa = lf.artist_mbid_frequencies()
		# fl = lf.album_mbid_frequencies()
		ft = lf.track_mbid_frequencies()
		# appending a padding zero for having the datatype structure

		# ft = [i + (0, ) for i in ft.items()]
		# appending key/value frequencies to previous ones
		cf = np.append(cf, np.array(ft.items(), dtype = npydt))
		
	# print 'RANK ', rank, ' FINISHED LOADING in TIME: ', time.time() - t0


	t0 = time.time()
	# Grouping frequencies by key in local process
	cf = np.array(GVM_classes.group_by_key(cf), dtype = npydt)
	# print 'RANK', rank, 'UNIQUE KEYS', cf.size, 'in TIME: ', time.time() - t0

	# number of keys in each process
	sendmsg = cf.size 
	# comm.Barrier() 	
	cf_size_all = comm.reduce(sendmsg, op=MPI.SUM, root = 0)

	

	if rank == 0:
		print 'CF_SIZE_ALL', cf_size_all
		# The global array size has to be multiple of number
		# of processes, thats why the modulo 
		# bs = cf_size_all + ( size - ( cf_size_all % size ) ) #
		# buffer_size = bs + ( 48 - ( bs % 48 ) ) # Next multiple of datatype size (48 bytes)
		buffer_size = cf_size_all + ( size - ( cf_size_all % size ) )
		# buffer_size = cf_size_all
		print 'BUFFER SIZE: ', buffer_size
		cf_all = np.zeros((buffer_size, ), dtype = npydt)

	else:

		cf_all = None

	# comm.Barrier() 

	print 'RANK', rank, 'CF', cf
	print 'CF_ALL', cf_all
	out = comm.Gather(sendbuf = [cf, mpidt], recvbuf = [cf_all, mpidt], root = 0)
	print '4'
	print 'out', out
	print 'RANK', rank, 'GATHERED\n', cf_all
	




	if rank == 0:

		t0 = time.time()
		cf_all = np.array(GVM_classes.group_by_key(cf_all), dtype = npydt)
		print 'FINAL GROUPING:\n', cf_all
		print 'FINAL NO UNIQUE KEYS', cf_all.size

		print 'FINAL GROUPING TIME', time.time() - t0

		# # Dumping data to file
		# outname = 'ranking_tracks_8T_' + str(SQ_JOBID) + '.dump'
		# cf_all.dump(outname)
		# # with open('ranking.pickle', 'wb') as handle:
  # # 			cPickle.dump(cf_all, handle)


















