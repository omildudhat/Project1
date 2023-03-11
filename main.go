package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)


var ErrInvalidArgs error = errors.New("invalid arguments")

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	// Shortest Job First (SJF) scheduling
	SJFSchedule(os.Stdout, "Shortest-job-first (SJF)", processes)

	// SJF with Priority scheduling
	SJFPrioritySchedule(os.Stdout, "SJF with Priority scheduling", processes)

	// Round-robin scheduling
	RRSchedule(os.Stdout, "Round-robin scheduling", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//func SJFPrioritySchedule(w io.Writer, title string, processes []Process) { }
//
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		remaining       = make([]Process, len(processes))
	)

	copy(remaining, processes)
	sort.Slice(remaining, func(i, j int) bool {
		if remaining[i].BurstDuration == remaining[j].BurstDuration {
			return remaining[i].Priority < remaining[j].Priority
		}
		return remaining[i].BurstDuration < remaining[j].BurstDuration
	})

	var running Process
	for len(remaining) > 0 || running.ProcessID != 0 {
		// add any arriving processes to the queue
		for len(remaining) > 0 && remaining[0].ArrivalTime <= serviceTime {
			if remaining[0].Priority < running.Priority {
				// preempt the running process if a higher-priority process arrives
				if running.ProcessID != 0 {
					queue := append([]Process{running}, remaining[0])
					remaining = append(remaining[1:], queue...)
				} else {
					running = remaining[0]
				}
			} else {
				remaining = append(remaining[1:], remaining[0])
			}
		}

		if running.ProcessID == 0 {
			// wait for the next process to arrive
			running = remaining[0]
			remaining = remaining[1:]
			waitingTime = serviceTime - running.ArrivalTime
			totalWait += float64(waitingTime)
		}

		start := serviceTime

		// execute the process for a fixed time slice
		if running.BurstDuration > 0 {
			running.BurstDuration--
			serviceTime++
			if running.BurstDuration == 0 {
				turnaround := waitingTime + running.BurstDuration + 1
				totalTurnaround += float64(turnaround)
				completion := serviceTime
				lastCompletion = float64(completion)
				schedule[running.ProcessID-1] = []string{
					fmt.Sprint(running.ProcessID),
					fmt.Sprint(running.Priority),
					fmt.Sprint(running.BurstDuration),
					fmt.Sprint(running.ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}
				running = Process{}
			}
		} else {
			// process has finished executing
			turnaround := waitingTime + running.BurstDuration + 1
			totalTurnaround += float64(turnaround)
			completion := serviceTime
			lastCompletion = float64(completion)
			schedule[running.ProcessID-1] = []string{
				fmt.Sprint(running.ProcessID),
				fmt.Sprint(running.Priority),
				fmt.Sprint(running.BurstDuration),
				fmt.Sprint(running.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			running = Process{}
		}

		gantt = append(gantt, TimeSlice{
			PID:   running.ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//func SJFSchedule(w io.Writer, title string, processes []Process) { }
//
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		remaining       = make([]Process, len(processes))
	)

	copy(remaining, processes)
	sort.Slice(remaining, func(i, j int) bool {
		return remaining[i].BurstDuration < remaining[j].BurstDuration
	})

	for len(remaining) > 0 {
		var next Process
		if len(remaining) == 1 || remaining[0].ArrivalTime <= serviceTime {
			next = remaining[0]
			remaining = remaining[1:]
		} else {
			// find the process with the shortest burst time that has arrived
			for i, p := range remaining {
				if p.ArrivalTime > serviceTime {
					break
				}
				if p.BurstDuration < next.BurstDuration || next.ProcessID == 0 {
					next = remaining[i]
				}
			}
			remaining = removeProcess(remaining, next)
		}

		if next.ProcessID != 0 {
			if next.ArrivalTime > 0 {
				waitingTime = serviceTime - next.ArrivalTime
			}
			totalWait += float64(waitingTime)

			start := waitingTime + next.ArrivalTime

			turnaround := next.BurstDuration + waitingTime
			totalTurnaround += float64(turnaround)

			completion := next.BurstDuration + next.ArrivalTime + waitingTime
			lastCompletion = float64(completion)

			schedule[next.ProcessID-1] = []string{
				fmt.Sprint(next.ProcessID),
				fmt.Sprint(next.Priority),
				fmt.Sprint(next.BurstDuration),
				fmt.Sprint(next.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			serviceTime += next.BurstDuration

			gantt = append(gantt, TimeSlice{
				PID:   next.ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//func RRSchedule(w io.Writer, title string, processes []Process) { }

func RRSchedule(w io.Writer, title string, processes []Process) {
	const quantum = 2 // fixed time slice

	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		remaining       = make([]Process, len(processes))
		queue           = make([]Process, 0)
	)

	copy(remaining, processes)

	for len(remaining) > 0 || len(queue) > 0 {
		// add any arriving processes to the queue
		for len(remaining) > 0 && remaining[0].ArrivalTime <= serviceTime {
			queue = append(queue, remaining[0])
			remaining = remaining[1:]
		}

		if len(queue) == 0 {
			// wait for the next process to arrive
			serviceTime = remaining[0].ArrivalTime
		} else {
			p := queue[0]
			queue = queue[1:]

			if p.ArrivalTime > 0 {
				waitingTime = serviceTime - p.ArrivalTime
			}
			totalWait += float64(waitingTime)

			start := waitingTime + p.ArrivalTime

			var completion int64
			if p.BurstDuration > quantum {
				p.BurstDuration -= quantum
				completion = serviceTime + quantum
				queue = append(queue, p)
			} else {
				completion = serviceTime + p.BurstDuration
				p.BurstDuration = 0
				lastCompletion = float64(completion)
			}

			turnaround := p.BurstDuration + waitingTime
			totalTurnaround += float64(turnaround)

			schedule[p.ProcessID-1] = []string{
				fmt.Sprint(p.ProcessID),
				fmt.Sprint(p.Priority),
				fmt.Sprint(p.BurstDuration),
				fmt.Sprint(p.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			serviceTime += quantum

			gantt = append(gantt, TimeSlice{
				PID:   p.ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}
