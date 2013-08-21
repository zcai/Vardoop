Vardoop
=======

A parallel variant calling tool on Hadoop

Current approaches to parallelizing variant calling:
• Split large job to smaller ones on LSF, SGE, Condor or other clusters
• Limitations
	- Data is located in a shared disk, and each computing node needs to read  data from the same disk.
	- Load balance in the cluster is not guaranteed.

Advantages compared with traditional grid computing:
Data locality

Move code to computing nodes instead of moving data to computing nodes.

• Load balance guaranteed

• Fault tolerance

• User friendly

A customized AMI (amazon machine image) with tools and reference data sets loaded was created and released.
