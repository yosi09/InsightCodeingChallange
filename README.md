# InsightCodingChallange

The code analyzes internet traffic data and provide the following outputs:

feature 1 - hosts.txt, 
feature 2 - resources.txt, 
feature 3 - hours.txt, 
and feature 4 - blocked.txt. 

The code received a log data file log.txt. To conserve memory only 100,000 lines are uploaded each time (changeable by changing CHUNKDATA) and repeat it for the whole file. 

Since only feature 4 needs to give real-time results while the other can be analyzed when the system is idle, feature 4 is analyzed by line, and therefore takes the longest to calculate.

I add a fifth feature (Needed to activate by removing comments) that similar to feature 3 but with no overlap between the windows: For example, feature 3 can give the following windows:
19:00:00 - 20:00:00; 
19:00:01 - 20:00:01; 
19:00:02 - 20:00:02; 

While feature 5:
19:00:00 - 20:00:00; 
20:00:00 - 21:00:00; 

I used to following standard packages in Python:

Python version 3.5.3 (v3.5.3:1880cb95a742, Jan 16 2017, 16:02:32) [MSC v.1900 64 bit (AMD64)]

Pandas version 0.19.2

NumPy version 1.12.1rc1

DateTime 4.0.1

The running time on my PC for each feature for the large log.txt file (in seconds):

Feature 1: computation time 187.17270636558533

Feature 2: computation time 25.460456132888794

Feature 3: computation time 39.75727391242981

Feature 4: computation time 3170.6553514003754


The code passed all 4 tests
