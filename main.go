package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

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

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
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

	ProcessData struct {
		TotalWait int64
		TAround   int64
		ExitTime  int64
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

func CheckIfDone(pd []ProcessData) bool { // if any of the process have not been finished
	for _, x := range pd {
		if x.ExitTime == 0 { // exit time zero means it never started
			return false
		}
	}
	return true
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	TempProcesses := make([]Process, len(processes)) // make new array to manipulate without affecting parent
	copy(TempProcesses, processes)

	pd := make([]ProcessData, len(TempProcesses)) // new array to keep track of process data
	for i := range pd {
		pd[i] = ProcessData{TotalWait: 0, TAround: 0, ExitTime: 0}
	}

	var time, start int64 = 0, 0 // used to keep track of the current time
	current := 0                 // keep track of current process being handled

	for !CheckIfDone(pd) { // while all processes are not finished
		swapped := false
		for index, proc := range pd { // at the start of the each cycle
			if TempProcesses[index].ArrivalTime < time { // if process has arrived
				if index != current && proc.ExitTime == 0 { // if it is not currently being worked
					pd[index].TotalWait += 1 //increase wait time by one
				} else if index == current { // if the process is currently being worked on
					TempProcesses[index].BurstDuration--
					if TempProcesses[index].BurstDuration == 0 {
						swapped = true
						pd[index].ExitTime = time
					}
				}
			}
		}
		new := 0
		for index, proc := range processes {
			if pd[index].ExitTime == 0 && proc.ArrivalTime <= time { // if the process is not already finished, and it has arrived
				// if the process at the index has a shorter burst time than the currently running one, or the current is finished, or there is a tie and the new process has a higher priortiy
				if TempProcesses[index].BurstDuration < TempProcesses[current].BurstDuration || // if the process has a shorter burst duration than the current one
					TempProcesses[current].BurstDuration < 1 || // if the current task is finished
					(TempProcesses[index].BurstDuration <= TempProcesses[current].BurstDuration && TempProcesses[index].Priority > TempProcesses[current].Priority) { //if the process have equal duration left, use priority as a tie breaker
					new = index
					swapped = true
				}
			}
		}
		if swapped { // if the current process has lost priority or the last one is done
			gantt = append(gantt, TimeSlice{ // place previous process in gantt table before switching processes
				PID:   int64(current + 1),
				Start: start,
				Stop:  time,
			})
			current = new // set the the process to be currently working
			start = time  // set the time
		}

		time++ // increment time
	}

	for i, proc := range pd {
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(proc.TotalWait),
			fmt.Sprint(proc.TotalWait + processes[i].BurstDuration),
			fmt.Sprint(proc.ExitTime),
		}

		totalTurnaround += float64(proc.TotalWait) + float64(processes[i].BurstDuration) // get total turnaround time
		totalWait += float64(proc.TotalWait)
	}
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / float64(time-1) //final time will be one less than counted time

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}



func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	TempProcesses := make([]Process, len(processes)) // make new array to manipulate without affecting parent
	copy(TempProcesses, processes)

	pd := make([]ProcessData, len(TempProcesses)) // new array to keep track of process data
	for i := range pd {
		pd[i] = ProcessData{TotalWait: 0, TAround: 0, ExitTime: 0}
	}

	var time, start int64 = 0, 0 // used to keep track of the current time
	current := 0                 // keep track of current process being handled

	for !CheckIfDone(pd) { // while all processes are not finished
		swapped := false
		for index, proc := range pd { // at the start of the each cycle
			if TempProcesses[index].ArrivalTime < time { // if process has arrived
				if index != current && proc.ExitTime == 0 { // if it is not currently being worked
					pd[index].TotalWait += 1 //increase wait time by one
				} else if index == current { // if the process is currently being worked on
					TempProcesses[index].BurstDuration--
					if TempProcesses[index].BurstDuration == 0 {
						fmt.Printf("%d finished: Swapped Triggered\n",index+1)
						swapped = true
						pd[index].ExitTime = time
					}
				}
			}
		}
		new := 0
		for index, proc := range processes {
			if(proc.ArrivalTime >= time && pd[index].ExitTime == 0){ 
				if(pd[new].ExitTime != 0){
					new = index
					swapped = true
				} else if(TempProcesses[index].BurstDuration < TempProcesses[new].BurstDuration){
					new = index
				swapped = true
				}
				
			}


			// if pd[index].ExitTime == 0 && proc.ArrivalTime <= time { // if the process is not already finished, and it has arrived
			// 	if TempProcesses[index].BurstDuration < TempProcesses[current].BurstDuration || TempProcesses[current].BurstDuration < 1 { // if the process at the index has a shorter burst time than the currently running one, or the current is finished
			// 		if(swapped || index == new){
			// 			if(TempProcesses[index].BurstDuration < TempProcesses[new].BurstDuration && TempProcesses[index].BurstDuration > 0){
						
			// 				fmt.Printf("%d going to %d\n",new+1,index+1)
			// 				new = index
			// 			}
			// 		}else	{
			// 			fmt.Printf("%d direct going to %d\n",new+1,index+1)
			// 			new = index
			// 		swapped = true
			// 		}
			// 		// 	fmt.Printf("%d new going to %d\n",new+1,index+1)
			// 		// 	new = index
			// 		// swapped = true
			// 		// }

			// 		new = index
			// 		swapped = true
			// 	}
			// }
		}
		if swapped { // if the current process has lost priority or the last one is done
			gantt = append(gantt, TimeSlice{ // place previous process in gantt table before switching processes
				PID:   int64(current + 1),
				Start: start,
				Stop:  time,
			})
			fmt.Printf("%d slice to %d\n",current+1,new+1)
			current = new // set the the process to be currently working
			start = time  // set the time
		}

		time++ // increment time
	}

	for i, proc := range pd {
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(proc.TotalWait),
			fmt.Sprint(proc.TotalWait + processes[i].BurstDuration),
			fmt.Sprint(proc.ExitTime),
		}

		totalTurnaround += float64(proc.TotalWait) + float64(processes[i].BurstDuration) // get total turnaround time
		totalWait += float64(proc.TotalWait)
	}
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / float64(time-1) //final time will be one less than counted time

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}

func getNextProcess(pd []ProcessData, proc []Process, current int, time int64) int {
	counter, max := 0, len(proc) // intiate variables
	current++
	if current >= max {
		current = 0
	}

	for counter < max {
		if pd[current].ExitTime == 0 && proc[current].ArrivalTime <= time { // if the process isn't done and has arrived
			return current
		} else {
			if current < (max - 1) { // increment to next one
				current++
			} else {
				current = 0
			}
			counter++
		}
	}
	return -1 // if all jobs are done

}

func RRSchedule(w io.Writer, title string, processes []Process) {

	var (
		totalWait       float64
		totalTurnaround float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	quantum := 0

	TempProcesses := make([]Process, len(processes)) // make new array to manipulate without affecting parent
	copy(TempProcesses, processes)

	pd := make([]ProcessData, len(TempProcesses)) // new array to keep track of process data
	for i := range pd {
		pd[i] = ProcessData{TotalWait: 0, TAround: 0, ExitTime: 0}
	}

	var time, start int64 = 0, 0                      // used to keep track of the current time
	current := getNextProcess(pd, processes, 0, time) // keep track of current process being handled
	for current > -1 {
		for index, proc := range pd { // at the start of the each cycle
			if TempProcesses[index].ArrivalTime < time { // if process has arrived
				if index != current && proc.ExitTime == 0 { // if it is not currently being worked
					pd[index].TotalWait += 1 //increase wait time by one
				} else if index == current { // if the process is currently being worked on
					TempProcesses[index].BurstDuration--
					if TempProcesses[index].BurstDuration == 0 {
						pd[index].ExitTime = time
					}
				}
			}
		}

		if quantum < 2 && pd[current].ExitTime == 0 { // if under the time quantum(2) and has not finished
			quantum++
		} else {
			quantum = 1
			next := getNextProcess(pd, processes, current, time) // get the next index in the round robin
			if next != current {                                 // if the new pid is not the same as the current update gantt
				gantt = append(gantt, TimeSlice{
					PID:   processes[current].ProcessID,
					Start: start,
					Stop:  time,
				})
				start = time
				current = next
			}
		}
		time++
	}

	for i, proc := range pd {
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(proc.TotalWait),
			fmt.Sprint(proc.TotalWait + processes[i].BurstDuration),
			fmt.Sprint(proc.ExitTime),
		}

		totalTurnaround += float64(proc.TotalWait) + float64(processes[i].BurstDuration) // get total turnaround time
		totalWait += float64(proc.TotalWait)
	}
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / float64(time-1) //final time will be one less than counted time

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

//endregion

//region Loading processes.

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

//endregion
