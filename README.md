# Cquirrel_Implementation
This is my implementation of [Cquirrel](https://cse.hkust.edu.hk/~yike/Cquirrel.pdf) using Flink. The Query 3 of [TPC-H](https://www.tpc.org/tpch/) is selected to implement.

## Dependencies
+ Java 11

+ Flink 1.15.3

## Data
The data can be generated by following the instruction [here](https://github.com/hkustDB/Cquirrel-release).

## Run the code
+ To run it in the local development mode (such as in IntelliJ): create a Maven project and put all file inside the **source_code** folder into it. Then build the project and run the main function inside **Cquirrel.java** with 2 arguments: `<path_to_your_csv>` `<path_to_output_folder>`. A txt file will be output in your desired output folder.

+ To run it in a cluster mode: download the jar [here](https://drive.google.com/file/d/1Kip3pwCEl0JabmOvcY8RLF4jrPvo_mpE/view?usp=drive_link). Submit the job via the webUI of Flink or use the cmd:
  
  `./flink run -d -c org.example.Cquirrel <path_to_jar> <path_to_your_csv> <path_to_output_folder>`

## Experiment result

Experiment is conducted on a **2-core, 4-thread Intel i7-7660U CPU**. Different settings (data scale factor (SF) & parallelism) are tested.

![Table of running time](https://github.com/WU-James/Cquirrel_Implementation/blob/main/figure/table.png)

The raw experiment result of running time (in sec) of Cquirrel algorithm.  

![Figure of running time](https://github.com/WU-James/Cquirrel_Implementation/blob/main/figure/figure.png)

The cluster bar chart of running time (in sec) of Cquirrel algorithm. Left: clustered by parallelism. Right: clustered by SF.


