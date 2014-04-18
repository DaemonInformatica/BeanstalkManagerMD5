BeanstalkManagerMD5
===================

Part of a network of programs to bruteforce MD5 hashes using beanstalkd This is the manager that creates jobs.

Why did I write this: 
One of my favorite branches of computer science is parallel computing. The art of dividing CPU-resource intensive projects
over multiple / many computers and gathering the results as pieces of the puzzle to put back together. Somebody introduced me
to the simple beanstalk-queue and I figured 'Lets think of an excuse to implement something around this. 

Thus the MD5 bruteforce hash-cracker was born. 

To use this, you need to download 3 projects in total: 
- BeanstalkManagerMD5 (This project) 
- BeanstalkWorkerDecodeMD5
- BeanstalkWorkerResult
- webservice database installation and webconsole. 


What does the BeanstalkManagerMD5 do? 
This project forms the process that manages which jobs are sent to which queues, at what timing. It keeps an eye on how many
processes are already running and generates the ranges for the workers to have a crack at. (pun intended) 
The code should be pretty self-explanatory, but basically the following happens: 

On start:
- connect to a database. 
- connect to the beanstalk service. 
- start loop. 

For each loop: 
- get uncracked md5's. 
- for each md5: 
  - check if a new process should be started (based on max-threads per user) 
  - get end of last range
  - craete new range.
  - push job to appropriate queue. 

It's important to note that the manager does NOT receive the decoder-worker's results! The decoders process their ranges and 
push the result back onto the beanstalk in a specific queue, which the BeanstalkWorkerResult listens to. 


