Application to reproduce OOME when caching huge blocks with MEMORY_AND_DISK_2 storage level.
 
All docker-related stuff is based on: https://github.com/big-data-europe/docker-spark

Steps to reproduce issue:
1) docker build --rm=true -t oome .
2) docker-compose -p oome up
3) docker run  --link spark-master:spark-master --network oome_default -e "STORAGE_LEVEL=MEMORY_AND_DISK_2" oome

Check for error messages in app's output and executor's heap dumps in working dir.

To run with other storage level:
docker run  --link spark-master:spark-master --network oome_default -e "STORAGE_LEVEL=MEMORY_AND_DISK" oome