# Cquirrel_Implementation
This is my implementation of [Cquirrel](https://cse.hkust.edu.hk/~yike/Cquirrel.pdf) using Flink.

## Dependencies
Java 11

Flink 1.15.3

## Run the code
To run it in the local development mode (such as in IntelliJ), create a Maven project and put all file inside the source_code folder into it. Then build the project and run the main function inside Cquirrel.java with 2 arguments: **<path_to_your_csv> <path_to_output_folder>**. The code will generate a txt file in your desired output folder.

To run it in a cluster mode, download the jar [here](https://drive.google.com/file/d/1Kip3pwCEl0JabmOvcY8RLF4jrPvo_mpE/view?usp=drive_link). Submit the job via the webUI of Flink or use the cmd:

**./flink run -d -c org.example.Cquirrel <path_to_jar> <path_to_your_csv> <path_to_output_folder>**

