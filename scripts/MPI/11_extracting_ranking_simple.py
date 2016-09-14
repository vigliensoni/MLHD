#! /opt/sharcnet/python/2.7.5/intel/bin/python

# sqsub -r 1h -q mpi -f xeon --mpp 3GB -o /work/vigliens/GV/8_RESULTS/rankings_no_copy_60m_3g_2n.log -n 8 python 4_SCRIPTS/MPI/11_extracting_ranking_simple.py 0 /work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB/ /scratch/vigliens/GV/8_RESULTS/

# for ((i=0; i<=5;i++)); do echo `sqsub -r 2h -q mpi --mpp 4GB -o /work/vigliens/GV/8_RESULTS/overall_rankings_women_2h_4g_96n_saw.log -n 96 python 4_SCRIPTS/MPI/11_extracting_ranking_simple.py $i /work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB/ /scratch/vigliens/GV/8_RESULTS/ `; done




# ##################################################
# The tinkerer solution, not reducing anything
# ##################################################

from mpi4py import MPI 
import sys, os
import tarfile, gzip
from optparse import OptionParser
import tempfile
import GVM_classes, Features
import time
import numpy as np
import cPickle
import shutil
import copy

if __name__ == "__main__":
	usage = "usage: %prog [options] factor input_folder output_folder feature"
	opts = OptionParser(usage = usage)
	# opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# ###########################################
	#  ARGS
	# ###########################################

	if len(args) != 3:
		print 'You should provide a:\n\n1) factor\n2) input_folder\n3) output_folder\n'
		sys.exit("\nEnter the proper values\n")

	factor = args[0]
	input_folder = args[1]
	output_folder = args[2]

	# ###########################################
	#  MPI
	# ###########################################

	comm = MPI.COMM_WORLD 
	size = comm.Get_size() 
	rank = comm.Get_rank() 

	TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
	# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]

	try:
		SQ_JOBID = os.environ["SQ_JOBID"]
	except:
		SQ_JOBID = '_STARR'


	# ##########################################################
	# Extracts all files within a TAR to TEMP_FOLDER
	# TODO: Extract only in one node? (which one) and bcast to 
	# the other ones
	# ##########################################################

	t0 = time.time()
	file_list = [] # List of all files in input_folder
	for root, subFolders, files in os.walk(input_folder):
		for f in files:
			if f == '.DS_Store':
				continue
			file_list.append('/'.join([root,f]))
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	print size * int(factor) + rank, file_list[size * int(factor) + rank], ' untared in ', int(time.time() - t0), 'secs'

	# ##########################################################
	# Iterate over all files in a TAR, searching for all MBIDs
	# ##########################################################

	# NumPy structured datatype 
	# npydt = np.dtype({'mbid': ('S36', 0), 'pad' : ('|V4', 36), 'freq': (np.int64, 40)})
	npydt = np.dtype({'mbid': ('|S40', 0), 'freq': ('<i8', 40)})
	# print 'NPYDT', npydt.itemsize

	# mpidt = MPI.Datatype.Create_struct( 
	# 	[40, 1], # block lengths 
	# 	[0, 40], # displacements in bytes
	# 	[MPI.CHAR, MPI.LONG], # MPI datatypes 
	# ).Commit() # don't forget to call Commit() after creation !!! 
	# # print 'MPI datatype size: ', mpidt.Get_size()


	# initializing arrays
	cf_ar = np.zeros((), dtype = npydt)
	cf_ar_dummy = np.zeros((), dtype = npydt)
	cf_al = np.zeros((), dtype = npydt)
	cf_al_dummy = np.zeros((), dtype = npydt)
	cf_tr = np.zeros((), dtype = npydt)
	cf_tr_dummy = np.zeros((), dtype = npydt)

	# For each file in the folder extract features and add to list
	t0 = time.time()
	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER):

		# Checking files starting with dot
		if file_in_tar.split('/')[-1].startswith('.'):
			continue

		# instance listening features
		lf = Features.ListeningFeatures(file_in_tar)

		# Checking for female users
		if lf.gender is not "m":
			continue


		f_ar = lf.artist_mbid_frequencies()
		f_al = lf.album_mbid_frequencies()
		f_tr = lf.track_mbid_frequencies()

		# appending key/value frequencies to previous ones
		# and making shallow copy to avoid memory leak
		cf_ar = np.append(cf_ar, np.array(f_ar.items(), dtype = npydt))
		del f_ar
		# cf_ar = copy.copy(cf_ar_dummy)
		# del cf_ar_dummy

		cf_al = np.append(cf_al, np.array(f_al.items(), dtype = npydt))
		del f_al
		# cf_al = copy.copy(cf_al_dummy)
		# del cf_al_dummy

		cf_tr = np.append(cf_tr, np.array(f_tr.items(), dtype = npydt))
		del f_tr
		# cf_tr = copy.copy(cf_tr_dummy)
		# del cf_tr_dummy
	print 'Rank ', rank, ' features in ', str(int(time.time() - t0)), 'secs'



	
	# Grouping frequencies by key in local process
	t0 = time.time()
	cf_ar = np.array(GVM_classes.group_by_key(cf_ar), dtype = npydt)
	cf_al = np.array(GVM_classes.group_by_key(cf_al), dtype = npydt)
	cf_tr = np.array(GVM_classes.group_by_key(cf_tr), dtype = npydt)
	print 'Rank ', rank, ' grouped in ', str(int(time.time() - t0)), 'secs'

	# Should I create a dictionary from the Numpy arrays?


	# Check directories
	if not os.path.isdir(output_folder + 'ranking_artists/'): os.mkdir(output_folder + 'ranking_artists/')
	if not os.path.isdir(output_folder + 'ranking_albums/'): os.mkdir(output_folder + 'ranking_albums/')
	if not os.path.isdir(output_folder + 'ranking_tracks/'): os.mkdir(output_folder + 'ranking_tracks/')

	# Dumping data to file
	outname_ar = output_folder + 'ranking_artists/' + str(SQ_JOBID) + '_' + str(rank) +  '.dump'
	outname_al = output_folder + 'ranking_albums/' + str(SQ_JOBID) + '_' + str(rank) +  '.dump'
	outname_tr = output_folder + 'ranking_tracks/' + str(SQ_JOBID) + '_' + str(rank) +  '.dump'
	
	cf_ar.dump(outname_ar)
	cf_al.dump(outname_al)
	cf_tr.dump(outname_tr)



	# After all processes have finished, maybe delete the TEMP folder
	shutil.rmtree(TEMP_FOLDER)













