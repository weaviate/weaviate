package memwatch

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/sirupsen/logrus"
)

const (
	maxIOPSDefault = float64(16000) // max IOPS in cloud environments
	component      = "IOPSMonitor"
)

type IOPSLevels float64

type queue struct {
	internalQueue []int
	position      int
	average       float64
}

func newQueue(size int) *queue {
	return &queue{
		internalQueue: make([]int, size),
		position:      0,
	}
}

func (q *queue) push(entry int) {
	q.average += (float64(entry) - float64(q.internalQueue[q.position])) / float64(len(q.internalQueue))
	q.internalQueue[q.position] = entry
	q.position = (q.position + 1) % len(q.internalQueue)
}

type IOPSMonitor struct {
	enabled   bool
	device    string
	maxIOPS   float64
	lastCheck time.Time
	lastStat  disk.IOCountersStat
	IOPSQueue *queue
}

func newIOPSMonitor(logger logrus.FieldLogger) IOPSMonitor {
	maxIOPS := maxIOPSDefault
	if v := os.Getenv("MONITOR_DISK_MAX_IOPS"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err == nil {
			maxIOPS = float64(asInt)
		}
	}

	stats, err := disk.IOCounters()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"component": component,
		}).Warnf("Could not get disk stats, IOPS monitoring disabled: %v", err)
		return IOPSMonitor{enabled: false}
	}

	var device string
	if v := os.Getenv("MONITOR_DISK_DEVICE"); v != "" {
		device = v
		_, exists := stats[device]
		if !exists {
			devices := make([]string, 0, len(stats))
			for k := range stats {
				devices = append(devices, k)
			}

			logger.WithFields(logrus.Fields{
				"component": component,
			}).Warnf("Could not find device %q, IOPS monitoring disabled. Available devices are %v", device, devices)
			return IOPSMonitor{enabled: false}
		}
	} else {
		if len(stats) > 1 {
			devices := make([]string, 0, len(stats))
			for k := range stats {
				devices = append(devices, k)
			}
			logger.WithFields(logrus.Fields{
				"component": component,
			}).Warnf("More than one disk found, please set MONITOR_DISK_DEVICE to the appropriate device name from %v. IOPS monitoring disabled.", devices)
			return IOPSMonitor{enabled: false}
		}
		for k := range stats {
			device = k
			break
		}
	}

	return IOPSMonitor{
		maxIOPS:   maxIOPS,
		device:    device,
		enabled:   true,
		lastStat:  stats[device],
		lastCheck: time.Now(),
		IOPSQueue: newQueue(100),
	}
}

func (m *IOPSMonitor) obtainCurrentIOPS() {
	if !m.enabled {
		return
	}

	stats, err := disk.IOCounters()
	if err != nil {
		return
	}

	currentStat, exists := stats[m.device]
	if !exists {
		return
	}

	currentTime := time.Now()
	timeDiff := currentTime.Sub(m.lastCheck).Seconds()
	if timeDiff > 0 {
		totalOps := (currentStat.ReadCount + currentStat.WriteCount) -
			(m.lastStat.ReadCount + m.lastStat.WriteCount)
		iops := float64(totalOps) / timeDiff
		m.IOPSQueue.push(int(iops))
	}
	m.lastCheck = currentTime
	m.lastStat = currentStat
}

func (m *IOPSMonitor) IOPSOverloaded(level float64) error {
	if !m.enabled {
		return nil
	}

	if m.IOPSQueue.average/m.maxIOPS > level {
		return fmt.Errorf("IOPS overloaded: current %.2f, max %.2f", m.IOPSQueue.average, m.maxIOPS)
	}
	return nil
}
