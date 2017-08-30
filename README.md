# Simple Chord based Distributed Hash Table 
This project contains a submission for an assignment; requirement for the course CSE 586: Distributed Systems offered in Spring 2017 at State University of New York.

## Introduction
We design and implement a simple Distributed Hash Table based on Chord protocol. Although the design is based on Chord, it is a simplified version of Chord; meaning we would not implement finger tables and finger-based routing; and not handling node leaves/failures.

We shall be focussing on implementing the following:
  1. ID space partitioning/re-partitioning
  
  2. Ring-based routing
  
  3. Node joins
  
If there are multiple instances of the app, all instances should form a Chord ring and serve insert/query requests in a distributed fashion according to the Chord protocol

## CHORD Distributed Hash Table

![Image](https://github.com/darshanbagul/CHORD_DHT/blob/master/images/DHT_Chord_Lookup.jpg)

## Testing

We have been provided a [Grader](https://github.com/darshanbagul/CHORD_DHT/tree/master/Grader) script, that tests our implementation rigorously by spawning multiple threads or multiple Android emulators.

### Running the Grader Script

  1. Load the Project in Android Studio and create the apk file.
  
  2. Download the Testing Program for your platform.
  
  3. Please also make sure that you have installed the app on all the AVDs.
  
  4. Before you run the program, please make sure that you are running five AVDs. The following command handles this:
       ```   
          python run_avd.py 5
       ```
  
  5. Also make sure that the Emulator Networking setup is done. Tis is handled by executing the following command
       ```
          python set_redir.py 10000
       ```
  
  6. Run the grader as illustrated:
       ```
          $ chmod +x < grader executable>
          
          $ ./< grader executable> apk file path
       ```
  
  7. Run the grader script with ‘-h’ argument for viewing help topics.

## Credits

This project comprises of scripts developed by Networked Systems Research Group at The State University of New York. I thank Prof. Steve Ko for teaching the course and encouraging practical implementations of important concepts in Large Scale Distributed Systems.

## References

   1. [Distributed Systems: Concepts and Design (5th Edition)](https://www.pearsonhighered.com/program/Coulouris-Distributed-Systems-Concepts-and-Design-5th-Edition/PGM85317.html) 

   2. Coursera MOOC - [Cloud Computing Concepts - University of Illinois at Urbana-Champaign](https://www.coursera.org/learn/cloud-computing) by Dr. Indranil Gupta
