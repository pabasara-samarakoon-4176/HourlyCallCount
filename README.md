# Hadoop MapReduce: 911 Emergency Call Analysis - Hourly Call Volume per Primary Reason
## Project Overview
This project implements a custom MapReduce job on a dataset of 911 emergency calls. The primary goal is to analyze call patterns by counting the number of calls for each primary reason (e.g., EMS, TRAFFIC, FIRE) on an hourly basis. This provides insights into the time-of-day distribution of various emergency events.

- Samarakoon P. G. C. (EG/2020/4176)
- Dodangoda M.D.R.R (EG/2020/4314)

## Hadoop Environment
- Hadoop 3.4.1
- openjdk version "1.8.0_452"

## Prerequisites
- JDK
- Apache Maven
- Apache Hadoop

## Dataset
- Name: Emergency - 911 Calls
- Source: https://www.kaggle.com/datasets/mchirico/montcoalert

## Project Structure
HourlyCallCount/
├── pom.xml                   # Maven project configuration and dependencies
└── src/
    └── main/
        └── java/
            └── com/
                └── example/
                    └── calls/
                        ├── HourlyCallCountMapper.java    # Mapper logic
                        ├── HourlyCallCountReducer.java   # Reducer logic
                        └── HourlyCallCountDriver.java    # Job configuration and runner

## Setup and Installation
### EC2 Instance Setup
1. Launch an EC2 instance.
2. Select Ubuntu as the OS.
3. Choose an instance type (t3.medium recommended)
4. Configure security group that has all traffic allowed to inbound and outbound. (Ports 9000, 50070, 22, 8020, 9870, 8088)
5. Create a private key.
6. Launch the instance.
7. Connect using SSH.

## Installations
1. Update the packeges.
2. Change the hostname (recommended)
3. Reboot the instance.
4. Install Open JDK 8.
5. Download and Install Hadoop.

## Configuration
1. Edit `core-site.xml`.
2. Edit `yarn-site.xml`.
3. Edit `mapred-site.xml`.
4. Edit `hdfs-site.xml`.

## Launch Hadoop Cluster
1. Format and start `hdfs`.
2. Start `yarn`.
3. Start the job history server.
4. Monitor Java processes `jps`.
5. Accessing NameNode and Cluster Metrics Overview. (`publicdns:50070` and `publicdns:8088`)

## Running the MapReduce
1. Copy project files to EC2 Instance.
2. Upload the dataset to HDFS.
3. Build the MapReduce project.
4. Run the MapReduce job.
5. View the job output.
6. Retrieve Full Output File to Local Machine