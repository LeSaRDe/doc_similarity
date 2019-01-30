#!/bin/bash

# Set the time, which is the maximum time your job can run in HH:MM:SS
#SBATCH --time=06:00:00

# Set the number of nodes, and the number of tasks per node (up to 16 per node)
#SBATCH --nodes=16 --ntasks-per-node=1

# Set memory to 10G (may have to switch to mem-per-cpu)


# Set the partition to submit to (a partition is equivalent to a queue)
#SBATCH -p discovery_q

# Set the account
#SBATCH -A ndssl

# Set the walltime
#SBATCH -t 6:00:00

echo "Env setup"

. envsetup.sh

echo "Annotation..."

java usersimproj.UserSimTest EA9kAjiQqhgGVWFW-3cMoA 2015-01-01T00:00:00Z 2017-09-01T00:00:00Z

echo "Done!"

#./text_clean_up.sh $1 

