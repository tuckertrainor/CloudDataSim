## Copyright

This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.

Marian K. Iskander, Tucker Trainor, Dave W. Wilkinson, Adam J. Lee, Panos K. Chrysanthis, [Balancing Performance, Accuracy, and Precision for Secure Cloud Transactions](http://db10.cs.pitt.edu/pubserver/web/ViewPublication.php?PublicationUID=d7f4ec861366c356a4e8d1096260dc), IEEE Transactions on Parallel and Distributed Systems, 25(2):417-426, February 2014, Date of on-line publication: 02 July 2013.

## Authors

This code is primarily the work of Tucker Trainor, a student researcher in the Department of Computer Science, University of Pittsburgh, under the supervision of Dr. Adam J. Lee and Dr. Panos K. Chrysanthis, both of the Department of Computer Science, University of Pittsburgh. Certain Java source code was adapted from original code by Dr. Lee as noted in the comments of those files. 

### Disclaimer
The code was written in 2012 and compiled for Java SE 1.6.0 update 33 on Mac OS X 10.6.8, and is untested on more recent versions of Java and OS X. The authors no longer support this code.

## Description

A Java-based simulation of enforcing policy and data consistency of cloud transactions. The goal is to simulate an array of servers processing data transactions while subject to different commit protocols and authentication policies.

## Running the Simulation

For simplicity, the entirety of the PolicySim folder should reside on each machine or instance that will be used as a "server". All Java source code in the folder should be compiled into `.class` files, and thus will be able to serve as the PolicyServer, the Robot, or a CloudServer. The folder also contains two files which should be identical across servers: `parameters.txt` and `serverConfig.txt`. The `parameters.txt` file contains the variables of the simulation and is commented with the description and options (if applicable) of each variable. The `serverConfig.txt` contains the integer ID of each server, its IP address and port used for communication, and is also commented with minor instructions.

With these resources in place, you can begin to launch the servers (example commands are based on an OS X environment):

1. First launch the single PolicyServer with `java PolicyServer <V|v>`, where the optional `V` or `v` is a flag to run in "verbose" mode, logging server activity to the console. Be sure that the instance where you launch the PolicyServer matches IP address and port given in the `serverConfig.txt` file.
2. Launch as many instances of CloudServer required by the simulation as set in `parameters.txt` with `java CloudServer <Server Number> <V|v>`, where `Server Number` is the ID of the CloudServer matching that on the `serverConfig.txt` file (i.e., IP address and port agree with the instance this CloudServer is running on) and an optional "verbose" flag
3. Finally, when all of these resources from 1. and 2. are available, launch the Robot to begin the simulation. There are two ways to launch the Robot:
  - With `java Robot`, the Robot will launch using the simulation variables set in the `parameters.txt` file.
  - With `java Robot <PROOF> <VM> <PUSH> <OPMIN> <OPMAX>`, you can override these respective parameters found in the `parameters.txt` file. All five parameters must present in the arguments for the override to be accepted. The ability to override these five parameters may be of use in automating tests where other parameters and server configurations will not change.

Upon completion of the simulation, a timestamped log will be created in the Robot's PolicySim folder with data about the run. Common errors are generally handled gracefully and error logs may also be generated in some circumstances.