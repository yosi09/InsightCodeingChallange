# InsightCodeingChallange

The code analyzed internet traffic data and provide the following outputs:

feature 1 - hosts.txt, 
feature 2 - resources.txt, 
feature 3 - hours.txt, 
and feature 4 - blocked.txt. 

The code received the log data from a file log.txt. To conserve the computer memory only 100,000 lines are uploaded each time (changeable by changing CHUNKDATA) and repeat it for the whole file. Features 1-3 are analyzed as a bulk while Feature 4 are anaylzed by line. The reason for that is since feature 4 needs to give a real-time results while the other can be treated when the system is idle. 

I add a fifth feature (Needed to activate by removing comments) that similar to feature 3 but with no overlap between the windows: For example, feature 3 can give the following windows:
19:00:00 - 20:00:00; 
19:00:01 - 20:00:01; 
19:00:02 - 20:00:02; 

While feature 5:
19:00:00 - 20:00:00; 
20:00:00 - 21:00:00; 

The packages i used are:

Python version 3.5.3 (v3.5.3:1880cb95a742, Jan 16 2017, 16:02:32) [MSC v.1900 64 bit (AMD64)]

Pandas version 0.19.2

NumPy version 1.12.1rc1

DateTime 4.0.1

The running time on my PC for each feature:
