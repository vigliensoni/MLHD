#!/home/vigliens/python/bin/python

# sqsub -r 5m -f xeon -q mpi --mpp 2GB -o 21_graphlab -n 2 /home/vigliens/python/bin/python /home/vigliens/Documents/2_CODE/4_SCRIPTS/MPI/21_graphlab_toy_example.py

# for ((i=0; i<=3;i++)); do echo `sqsub -r 15m -f xeon -q mpi --mpp 2GB -o songfact -n 2 python 4_SCRIPTS/MPI/19_songfacts.py $i`; done

# for ((i=0; i<=72;i++)); do echo `sqsub -r 30m -f xeon -q mpi --mpp 2GB -o songfact -n 8 python 4_SCRIPTS/MPI/19_songfacts.py $i`; done

# for ((i=7269942; i<=7270026;i++)); do echo `sqkill $i`; done

# ##################################################
# This scripts should
# 1. Test graphlab in CC cluster
# ##################################################


from mpi4py import MPI
import os
from optparse import OptionParser
import tempfile
import graphlab as gl


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
print "SIZE:{0}, RANK:{1}".format(size, rank)

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens')  # ComputeCanada
# print 'TEMP_FOLDER: ', TEMP_FOLDER
# TEMP_FOLDER = tempfile.mkdtemp(dir='/Users/gabriel/scratch/') # Local

# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
try:
    SQ_JOBID = os.environ["SQ_JOBID"]
except:
    SQ_JOBID = '_STARR'

print 'SQ_JOBID:{0}'.format(SQ_JOBID)

if __name__ == "__main__":
    usage = "usage: %prog [options] factor"
    opts = OptionParser(usage=usage)
    # opts.add_option('-f', '--hdf5', dest='h5')
    options, args = opts.parse_args()

    sf = gl.SFrame.read_csv('/home/vigliens/Documents/1_toy_example/test_ratings.csv')

    user_data = gl.SFrame({'user_id': ["gabriel", "antonia", "santiago", "vito", "justina", "ich"],
                           'age': ["o", "o", "y", "y", "y", "o"],
                           'gender': ["m", "f", "m", "m", "f", "m"]})

    print user_data

    model = gl.factorization_recommender.create(
        observation_data=sf,
        user_id='user_id',
        item_id='item_id',
        target='rating',
        user_data=user_data,
        regularization=1e-05,
        num_factors=2)
