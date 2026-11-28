//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build postingedgebench

package postingedge

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// systemMemory is what macOS tells of its memory at one moment, untimed: the
// counters vm_stat keeps since the boot, in pages, and the pages its
// compressor holds; the share of the memory free that memory_pressure tells;
// the swap sysctl tells; and, of this process alone, what top tells of its
// memory in the compressor and of its footprint. It is the test of the
// hypothesis that the flat search of the night of the 23rd ran slower
// because macOS had compressed or swapped out the pages it reads.
type systemMemory struct {
	At             string  `json:"at"`
	PageBytes      int     `json:"pageBytes"`
	Decompressions uint64  `json:"decompressions"`
	Compressions   uint64  `json:"compressions"`
	Pageins        uint64  `json:"pageins"`
	Swapins        uint64  `json:"swapins"`
	Swapouts       uint64  `json:"swapouts"`
	InCompressor   uint64  `json:"inCompressor"`
	FreePercent    int     `json:"freePercent"`
	SwapUsedMB     float64 `json:"swapUsedMB"`
	SwapTotalMB    float64 `json:"swapTotalMB"`
	// ProcessCompressedMB and ProcessFootprintMB are top's CMPRS and MEM of
	// this process.
	ProcessCompressedMB float64 `json:"processCompressedMB"`
	ProcessFootprintMB  float64 `json:"processFootprintMB"`
	// Process is the accounting of the CPU of this process so far: whether
	// it ran on the efficiency cores, at a lower QoS, waiting for a core, or
	// at fewer cycles a second.
	Process processCPUTimes `json:"process"`
}

// processCPUTimes is the accounting of the CPU of a process since it started,
// in nanoseconds and counts: the times on the CPU, the part of them on the
// performance cores (P), the time runnable (running or waiting for a core:
// on an idle machine it is the time on the CPU), the time by QoS class (default, maintenance, background, utility, legacy, user
// initiated, user interactive), the instructions and the cycles, and the
// page-ins.
type processCPUTimes struct {
	UserNs        uint64    `json:"userNs"`
	SystemNs      uint64    `json:"systemNs"`
	UserPNs       uint64    `json:"userPNs"`
	SystemPNs     uint64    `json:"systemPNs"`
	RunnableNs    uint64    `json:"runnableNs"`
	QoSNs         [7]uint64 `json:"qosNs"`
	Instructions  uint64    `json:"instructions"`
	Cycles        uint64    `json:"cycles"`
	PInstructions uint64    `json:"pInstructions"`
	PCycles       uint64    `json:"pCycles"`
	Pageins       uint64    `json:"pageins"`
	// DiskReadBytes is what the system read from the disk for the process:
	// the reads of the rescore its file cache did not hold.
	DiskReadBytes uint64 `json:"diskReadBytes"`
}

// cpuChange tells what a process did on the CPU between two of its
// accountings: its time on the CPU, the share of it on the performance cores,
// the shares at background and utility QoS, the time it waited for a core,
// its instructions a cycle and its cycles a nanosecond on the CPU.
func cpuChange(before, after processCPUTimes) string {
	cpu := float64(after.UserNs + after.SystemNs - before.UserNs - before.SystemNs)
	share := func(of uint64) float64 { return 100 * float64(of) / max(cpu, 1) }
	cycles := float64(after.Cycles - before.Cycles)
	// The time runnable holds the time running: what is left is the wait for
	// a core.
	waited := max(float64(after.RunnableNs-before.RunnableNs)-cpu, 0)
	return fmt.Sprintf("%.2f s on the CPU, %.1f %% on the performance cores, %.1f %% at background QoS and %.1f %% "+
		"at utility, %.2f s runnable and waiting for a core, %.2f instructions a cycle, %.2f GHz on the CPU, %d "+
		"page-ins, %.0f MB read from the disk",
		cpu/1e9, share(after.UserPNs+after.SystemPNs-before.UserPNs-before.SystemPNs),
		share(after.QoSNs[2]-before.QoSNs[2]), share(after.QoSNs[3]-before.QoSNs[3]),
		waited/1e9, float64(after.Instructions-before.Instructions)/max(cycles, 1),
		cycles/max(cpu, 1), after.Pageins-before.Pageins, float64(after.DiskReadBytes-before.DiskReadBytes)/(1<<20))
}

// sampleSystemMemory runs vm_stat, memory_pressure -Q, sysctl vm.swapusage
// and top on this process, and reads them.
func sampleSystemMemory(ctx context.Context) (systemMemory, error) {
	run := func(name string, args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
		if err != nil {
			return "", errors.Wrapf(err, "%s %s: %s", name, strings.Join(args, " "), out)
		}
		return string(out), nil
	}
	m := systemMemory{At: time.Now().Format("15:04:05")}
	out, err := run("vm_stat")
	if err != nil {
		return m, err
	}
	if err := m.readVMStat(out); err != nil {
		return m, err
	}
	if out, err = run("memory_pressure", "-Q"); err != nil {
		return m, err
	}
	if m.FreePercent, err = readFreePercent(out); err != nil {
		return m, err
	}
	if out, err = run("sysctl", "-n", "vm.swapusage"); err != nil {
		return m, err
	}
	if m.SwapTotalMB, m.SwapUsedMB, err = readSwapUsage(out); err != nil {
		return m, err
	}
	if out, err = run("top", "-l", "1", "-pid", strconv.Itoa(os.Getpid()), "-stats", "pid,cmprs,mem"); err != nil {
		return m, err
	}
	if m.ProcessCompressedMB, m.ProcessFootprintMB, err = readTopMemory(out, os.Getpid()); err != nil {
		return m, err
	}
	if m.Process, err = processCPU(); err != nil {
		return m, err
	}
	return m, nil
}

// readVMStat reads the page size and the counters of the output of vm_stat.
func (m *systemMemory) readVMStat(out string) error {
	lines := strings.Split(out, "\n")
	const header = "(page size of "
	at := strings.Index(lines[0], header)
	if at < 0 {
		return errors.Errorf("vm_stat: no page size in %q", lines[0])
	}
	size, _, _ := strings.Cut(lines[0][at+len(header):], " ")
	pageBytes, err := strconv.Atoi(size)
	if err != nil || pageBytes <= 0 {
		return errors.Errorf("vm_stat: page size %q in %q", size, lines[0])
	}
	m.PageBytes = pageBytes
	counters := map[string]uint64{}
	for _, line := range lines[1:] {
		at := strings.LastIndex(line, ":")
		if at < 0 {
			continue
		}
		name := strings.Trim(strings.TrimSpace(line[:at]), `"`)
		value := strings.TrimSuffix(strings.TrimSpace(line[at+1:]), ".")
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.Errorf("vm_stat: %q is not a count in %q", value, line)
		}
		counters[name] = n
	}
	for name, to := range map[string]*uint64{
		"Decompressions":             &m.Decompressions,
		"Compressions":               &m.Compressions,
		"Pageins":                    &m.Pageins,
		"Swapins":                    &m.Swapins,
		"Swapouts":                   &m.Swapouts,
		"Pages stored in compressor": &m.InCompressor,
	} {
		n, ok := counters[name]
		if !ok {
			return errors.Errorf("vm_stat: no %q", name)
		}
		*to = n
	}
	return nil
}

// readFreePercent reads the share of the memory free in the output of
// memory_pressure -Q.
func readFreePercent(out string) (int, error) {
	const prefix = "System-wide memory free percentage:"
	for _, line := range strings.Split(out, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			value := strings.TrimSuffix(strings.TrimSpace(rest), "%")
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 || n > 100 {
				return 0, errors.Errorf("memory_pressure: %q is not a percentage in %q", value, line)
			}
			return n, nil
		}
	}
	return 0, errors.Errorf("memory_pressure: no %q in %q", prefix, out)
}

// readSwapUsage reads the swap, total and used, in MB, in the output of
// sysctl -n vm.swapusage ("total = 6144.00M  used = 4713.19M  free = ...").
func readSwapUsage(out string) (total, used float64, err error) {
	fields := strings.Fields(out)
	value := func(name string) (float64, error) {
		for i := 0; i+2 < len(fields); i++ {
			if fields[i] == name && fields[i+1] == "=" {
				return readMB(fields[i+2])
			}
		}
		return 0, errors.Errorf("vm.swapusage: no %q in %q", name, out)
	}
	if total, err = value("total"); err != nil {
		return 0, 0, err
	}
	if used, err = value("used"); err != nil {
		return 0, 0, err
	}
	return total, used, nil
}

// readTopMemory reads the CMPRS and MEM columns of the row of a process in
// the output of top -l 1 -pid <pid> -stats pid,cmprs,mem, in MB.
func readTopMemory(out string, pid int) (compressed, footprint float64, err error) {
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != strconv.Itoa(pid) {
			continue
		}
		if compressed, err = readMB(fields[1]); err != nil {
			return 0, 0, errors.Wrap(err, "top, CMPRS")
		}
		if footprint, err = readMB(fields[2]); err != nil {
			return 0, 0, errors.Wrap(err, "top, MEM")
		}
		return compressed, footprint, nil
	}
	return 0, 0, errors.Errorf("top: no row of pid %d in %q", pid, out)
}

// readMB reads a size as top and sysctl print it, a number with a unit of B,
// K, M, G or T and, from top, a trailing + or - when it grew or shrank, in MB.
func readMB(size string) (float64, error) {
	s := strings.TrimRight(size, "+-")
	units := map[byte]float64{'B': 1.0 / (1 << 20), 'K': 1.0 / (1 << 10), 'M': 1, 'G': 1 << 10, 'T': 1 << 20}
	if len(s) < 2 {
		return 0, errors.Errorf("%q is not a size", size)
	}
	unit, ok := units[s[len(s)-1]]
	if !ok {
		return 0, errors.Errorf("%q has no unit", size)
	}
	n, err := strconv.ParseFloat(s[:len(s)-1], 64)
	if err != nil || n < 0 {
		return 0, errors.Errorf("%q is not a size", size)
	}
	return n * unit, nil
}

// memoryChange tells what changed between two samples, for the events.
func memoryChange(before, after systemMemory) string {
	return fmt.Sprintf("decompressions +%d, compressions +%d, pageins +%d, swapins +%d, swapouts +%d | in the "+
		"compressor %d → %d pages | free %d %% → %d %% | swap used %.0f → %.0f MB of %.0f | this process: %.0f → "+
		"%.0f MB compressed, footprint %.0f → %.0f MB; %s", after.Decompressions-before.Decompressions,
		after.Compressions-before.Compressions, after.Pageins-before.Pageins, after.Swapins-before.Swapins,
		after.Swapouts-before.Swapouts, before.InCompressor, after.InCompressor, before.FreePercent, after.FreePercent,
		before.SwapUsedMB, after.SwapUsedMB, after.SwapTotalMB, before.ProcessCompressedMB, after.ProcessCompressedMB,
		before.ProcessFootprintMB, after.ProcessFootprintMB, cpuChange(before.Process, after.Process))
}

// TestProcessCPU checks the accounting of processCPU against getrusage while
// this goroutine spins on the CPU: the same time on the CPU, the time by QoS
// adding up to it, the time on the performance cores within it, and cycles
// and instructions counted.
func TestProcessCPU(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("the CPU of the process by core and QoS is only counted on macOS")
	}
	before, err := processCPU()
	require.NoError(t, err)
	usageBefore, err := processCPUTimeOfGetrusage()
	require.NoError(t, err)
	// It spins until getrusage counts 300 ms on the CPU, not for 300 ms of
	// the clock: on a busy machine the clock gives it less.
	spin := uint64(1)
	for start := time.Now(); ; {
		for range 1 << 16 {
			spin = spin*6364136223846793005 + 1442695040888963407
		}
		usage, err := processCPUTimeOfGetrusage()
		require.NoError(t, err)
		if usage-usageBefore >= 300*time.Millisecond {
			break
		}
		require.Less(t, time.Since(start), time.Minute, "300 ms on the CPU")
	}
	require.NotZero(t, spin)
	after, err := processCPU()
	require.NoError(t, err)
	usageAfter, err := processCPUTimeOfGetrusage()
	require.NoError(t, err)

	cpu := float64(after.UserNs + after.SystemNs - before.UserNs - before.SystemNs)
	usage := float64(usageAfter - usageBefore)
	require.InDelta(t, usage, cpu, 0.1*usage, "the time on the CPU of processCPU against getrusage: the timebase")
	var qos uint64
	for q := range after.QoSNs {
		qos += after.QoSNs[q] - before.QoSNs[q]
	}
	require.InDelta(t, cpu, float64(qos), 0.1*cpu, "the time by QoS adds up to the time on the CPU")
	require.LessOrEqual(t, float64(after.UserPNs+after.SystemPNs-before.UserPNs-before.SystemPNs), 1.01*cpu,
		"the time on the performance cores is within the time on the CPU")
	cycles := float64(after.Cycles - before.Cycles)
	require.Greater(t, cycles, 0.2*cpu, "at least 0.2 GHz while it spins")
	require.Less(t, cycles, 6*cpu, "at most 6 GHz")
	require.Greater(t, float64(after.Instructions-before.Instructions), 0.2*cycles, "instructions a cycle")
	require.LessOrEqual(t, after.PCycles-before.PCycles, after.Cycles-before.Cycles, "the cycles on the performance cores")
	t.Logf("%s", cpuChange(before, after))
}

func TestReadSystemMemory(t *testing.T) {
	// The outputs of the 24th of November at 07:20, on the machine of the
	// measure.
	const vmStat = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                              631472.
Pages active:                           1424025.
Pages stored in compressor:             1420271.
Pages occupied by compressor:            232365.
Decompressions:                      3811127164.
Compressions:                        3969235382.
Pageins:                             1144310571.
Pageouts:                              15274205.
Swapins:                              275326947.
Swapouts:                             286732426.
"Translation faults":               23308616010.
`
	t.Run("vm_stat", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			out  string
			want systemMemory
			err  string
		}{
			{
				name: "the machine of the measure", out: vmStat,
				want: systemMemory{
					PageBytes: 16384, Decompressions: 3811127164, Compressions: 3969235382, Pageins: 1144310571,
					Swapins: 275326947, Swapouts: 286732426, InCompressor: 1420271,
				},
			},
			{name: "no header", out: strings.SplitN(vmStat, "\n", 2)[1], err: "no page size"},
			{name: "a counter missing", out: strings.Replace(vmStat, "Swapins:", "Swapped:", 1), err: `no "Swapins"`},
			{name: "not a count", out: strings.Replace(vmStat, "275326947.", "many.", 1), err: "is not a count"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var m systemMemory
				err := m.readVMStat(tc.out)
				if tc.err != "" {
					require.ErrorContains(t, err, tc.err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, m)
			})
		}
	})

	t.Run("memory_pressure", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			out  string
			want int
			err  string
		}{
			{
				name: "the machine of the measure",
				out:  "The system has 68719476736 (4194304 pages with a page size of 16384).\nSystem-wide memory free percentage: 86%\n",
				want: 86,
			},
			{name: "no percentage", out: "The system has 68719476736.\n", err: "no"},
			{name: "not a percentage", out: "System-wide memory free percentage: 186%\n", err: "is not a percentage"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := readFreePercent(tc.out)
				if tc.err != "" {
					require.ErrorContains(t, err, tc.err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("vm.swapusage", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			out         string
			total, used float64
			err         string
		}{
			{
				name: "the machine of the measure", out: "total = 6144.00M  used = 4713.19M  free = 1430.81M  (encrypted)\n",
				total: 6144, used: 4713.19,
			},
			{name: "in gigabytes", out: "total = 8.00G  used = 0.50G  free = 7.50G\n", total: 8192, used: 512},
			{name: "no swap", out: "total = 0.00M  used = 0.00M  free = 0.00M\n"},
			{name: "no used", out: "total = 6144.00M  free = 1430.81M\n", err: `no "used"`},
			{name: "no unit", out: "total = 6144.00  used = 4713.19M\n", err: "has no unit"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				total, used, err := readSwapUsage(tc.out)
				if tc.err != "" {
					require.ErrorContains(t, err, tc.err)
					return
				}
				require.NoError(t, err)
				require.InDelta(t, tc.total, total, 1e-9)
				require.InDelta(t, tc.used, used, 1e-9)
			})
		}
	})

	t.Run("top", func(t *testing.T) {
		const out = "Processes: 712 total\n\nPID    CMPRS MEM   \n87356  19M   4342M \n"
		for _, tc := range []struct {
			name                  string
			out                   string
			pid                   int
			compressed, footprint float64
			err                   string
		}{
			{name: "the diagnostic of the 24th", out: out, pid: 87356, compressed: 19, footprint: 4342},
			{
				name: "grown and in kilobytes", out: "PID    CMPRS MEM\n12  512K+ 1G-\n", pid: 12,
				compressed: 0.5, footprint: 1024,
			},
			{name: "in bytes", out: "PID CMPRS MEM\n12 0B 3M\n", pid: 12, footprint: 3},
			{name: "another process", out: out, pid: 87357, err: "no row of pid 87357"},
			{name: "not a size", out: "PID CMPRS MEM\n12 N/A 3M\n", pid: 12, err: "CMPRS"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				compressed, footprint, err := readTopMemory(tc.out, tc.pid)
				if tc.err != "" {
					require.ErrorContains(t, err, tc.err)
					return
				}
				require.NoError(t, err)
				require.InDelta(t, tc.compressed, compressed, 1e-9)
				require.InDelta(t, tc.footprint, footprint, 1e-9)
			})
		}
	})
}
