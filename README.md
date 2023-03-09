Project 1: Process Scheduler
Description
For this project we'll be building a simple process scheduler that takes in a file containing example processes, and outputs a schedule based on the three different schedule types:

First Come First Serve (FCFS) [already done]
Shortest Job First (SJF)
SJF Priority
Round-robin (RR)
Assume that all processes are CPU bound (they do not block for I/O).
The scheduler will be written in Go (a skeleton main.go is included in the project repo).

Steps
Clone down the example input/output and skeleton main.go:

git clone https://github.com/jh125486/CSCE4600
Copy the Project1 files to your own git project.

The processes for your scheduling algorithms are read from a file as the first argument to your program.

Every line in this file includes a record with comma separated fields.

The format for this record is the following: <ProcessID>,<Burst Duration>,<Arrival Time>,<Priority>.
Not all fields are used by all scheduling algorithms. For example, for FCFS you only need the process IDs, arrival times, and burst durations.

All processes in your input files will be provided a unique process ID. The arrival times and burst durations are integers. Process priorities have a range of [1-50]; the lower this number, the higher the priority i.e. a process with priority=1 has a higher priority than a process with priority=2.

Start editing the main.go and add the scheduling algorithms:

Implement SJF (preemptive) and report average turnaround time, average waiting time, and average throughput.

Implement SJF priority scheduling (preemptive) and report average turnaround time, average waiting time, and average throughput.

Round-round (preemptive) and report average turnaround time, average waiting time, and average throughput.

Grading
Code must compile and run.

Each type is worth different points:

30 points for implementing FCFS (already done, so you get 30 points for just submitting)
25 points for implementing SJF [preemptive]
25 points for implementing SJF Priority scheduling [preemptive]
20 points for implementing RR [always preemptive]
Deliverables
A GitHub link to your project which includes:

README.md <- describes anything needed to build (optional)
main.go <- your scheduler