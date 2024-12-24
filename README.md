# Cquirrel_Implementation
This is my implementation of https://cse.hkust.edu.hk/~yike/Cquirrel.pdf using Flink.

## Dependencies
Java 11
Flink 1.15.3

## Run the code
To run it in the local development mode (such as in IntelliJ), create a Maven project and put all .java and pom.xml file into it. Then build the project and run the main function inside Cquirrel.java with 2 arguments: **<path_to_your_csv> <path_to_output_folder>**. The code will generate a txt file in your desired output folder.

To run it in a cluster mode, submit the jar via the webUI of Flink or use the cmd after you start the cluster: 

**./flink run -d -c org.example.Cquirrel <path_to_jar> <path_to_your_csv> <path_to_output_folder>**

