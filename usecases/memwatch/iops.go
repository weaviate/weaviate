package memwatch

import (
	"os"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/sirupsen/logrus"
)

const maxIOPSDefault = 16000 // max IOPS in cloud environments

type IOPSMonitor struct {
	enabled     bool
	device      string
	maxIOPS     int
	lastCheck   time.Time
	lastStat    disk.IOCountersStat
	currentIOPS int
}

func newIOPSMonitor(logger logrus.FieldLogger) IOPSMonitor {
	maxIOPS := maxIOPSDefault
	if v := os.Getenv("MONITOR_DISK_IOPS_MAX"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err == nil {
			maxIOPS = asInt
		}
	}

	stats, err := disk.IOCounters()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"component": "IOPSMonitor",
		}).Warnf("Could not get disk stats, IOPS monitoring disabled: %v", err)
		return IOPSMonitor{enabled: false}
	}

	var device string
	if v := os.Getenv("MONITOR_DISK_IOPS_DEVICE"); v != "" {
		device = v
		_, exists := stats[device]
		if !exists {
			logger.WithFields(logrus.Fields{
				"component": "IOPSMonitor",
			}).Warnf("Could not find device %q, IOPS monitoring disabled", device)
			return IOPSMonitor{enabled: false}
		}
	} else {
		if len(stats) > 1 {
			logger.WithFields(logrus.Fields{
				"component": "IOPSMonitor",
			}).Warnf("More than one disk found, please set MONITOR_DISK_IOPS_DEVICE to the appropriate device name. IOPS monitoring disabled.")
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
		m.currentIOPS = int(iops)
	}
	m.lastCheck = currentTime
	m.lastStat = currentStat
}

func (m *IOPSMonitor) IOPSOverloaded(expected int) bool {
	if m.currentIOPS+expected > m.maxIOPS {
		return true
	}
	return true
}
