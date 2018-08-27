For running the preprocessing code in Python, follow the following steps:
- Go to the Folder where the "preprocess.py" file is stored 

- Follow the following example to run the python code in Windows
 Eg: python preprocess.py C:\foods.txt C:\food.csv


For running the code in IntelliJ:
- Import the project into IntelliJ

- Click in Import Project

- Go to sbt projects tab and then double click on package 

- After successful build, the jar file will be placed in the target folder of the project

After the main jar is generated. The main jar file is attached. Please follow the steps to run the jar in Amazon Web Services:

- Copy the jar file into the AWS S3 bucket For eg: s3://finalProject/finalproject_2.11-0.1

- Then start the EMR cluster

- After successfully starting the Cluster, go to Steps and then create one by giving following information:

	- Set Step type: Spark application

	- Set some unique name for it

	- In SPark-submit options, For eg. give below for Part1: '--class "FinalProject"'

	- Select the Application location by selecting the jar file location

	- Then in the Arguments, For eg. For Part 1 and Part 2, give two arguments, first one for input file and second as Output folder. "s3://finalProject/food.csv s3://finalProject/Output".
	
- Then click on Add

- After sometime, when the task is completed, go to s3 bucket and see the Ouput folder as mentioned in the arguments and check for the Output file.

NOTE: The build.sbt file is attached with the jar file