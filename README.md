# Big-Data-Project1

## Project Description

Project 1 for Revature's Big Data Training. This application is a command-line fantasy football news tracker that supports the creation of accounts and allows users to view the currently trending fantasy and NFL news about the last week's games, next week's games, and current player injuries by using information from News API.

## Technologies Used
- Hadoop MapReduce
- YARN
- HDFS
- Scala 2.11.8
- Hive
- Git + GitHub

## Features
- User account creation
- User login/logout
- Basic user and admin user functionality (admin can manage other accounts and application data retrieval)
- User account management including password changes and name changes
- View top NFL fantasy football news for the current week
- View top NFL fantasy football news for the next week
- View top NFL injury news

To-do:
- Clean up data retrieval and parsing

Analysis questions to answer:
- Which players are projected to do the best for their upcoming game?
- Which top players are projected to do poorly in their upcoming game?
- Which players are newly injured (which may affect their performance)?
- Which players are returning from an injury (which may make them valuable to pick up)?
- Which players will be on a bye this coming week (and won't play)?
- Which players performed the best in their latest game?
- Which top players performed the worst compared to their expected performance in their last game?

## Getting Started
- Clone the repository:
```
$ git clone https://github.com/natshenk/Big-Data-Project1
```
- Navigate to the "Big-Data-Project1" folder and run sbt:
```
Big-Data-Project1> sbt
```
- Package the application:
```
sbt:Big-Data-Project1> package
```
- If using Hortonworks Sandox, move the "big-data-project1_2.11-0.1.0-SNAPSHOT.jar" file to Hadoop using WinSCP or the scp command in Shell In A Box:
```
$ scp <filepath>/big-data-project1_2.11-0.1.0-SNAPSHOT.jar <user@ip_address:/file/path>
```
- Run the application by using spark-submit:
```
$ spark-submit --packages net.liftweb:lift-json_2.11:2.6 big-data-project1_2.11-0.1.0-SNAPSHOT.jar
```
The application can then be interacted with by entering the appropriate menu selections in the console

## Contributors
Nate Shenk @natshenk
