//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package resources

import (
	"context"
	"fmt"
	"os"
	goruntime "runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/pbnjay/memory"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"
)

type Limits struct {
	lock          sync.Mutex
	cancelFunc    context.CancelFunc
	logger        logrus.FieldLogger
	previousLimit int64
}

func NewLimit(config config.ResourceLimits, logger logrus.FieldLogger) *Limits {
	if config.Enabled && config.EnabledDeprecated {
		logger.Warn("Both LIMIT_RESOURCES and LIMIT_RESOURCES_DYNAMIC_ENABLED are set to true. " +
			"LIMIT_RESOURCES is deprecated and will be ignored. Please use LIMIT_RESOURCES_DYNAMIC_ENABLED going forward.")
		config.EnabledDeprecated = false
	}
	limit := &Limits{
		lock:          sync.Mutex{},
		cancelFunc:    func() {},
		logger:        logger.WithField("action", "resource_limits"),
		previousLimit: 0, // only used to check if the limit was changed and if a log message needs to be emitted
	}

	if config.EnabledDeprecated {
		limitResourcesDeprecated(logger)
	} else if config.Enabled {
		if err := limit.ApplyResourceLimits(config)(); err != nil {
			logger.WithField("action", "startup").Warnf("Unable to parse GOMEMLIMIT: %v", err)
		}
	} else {
		logger.Info("No resource limits set, weaviate will use all available memory and CPU. " +
			"To limit resources, set LIMIT_RESOURCES_DYNAMIC_ENABLED=true")
	}

	return limit
}

func (l *Limits) Shutdown() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.cancelFunc()
}

func (l *Limits) ApplyResourceLimits(conf config.ResourceLimits) func() error {
	return func() error {
		l.lock.Lock()
		defer l.lock.Unlock()

		if goruntime.GOOS == "linux" && conf.GoMemLimitFromCgroupsRatio.Get() > 0 {
			l.applyCgroupLimit(conf) // run right away, the ticker will only fire after the waittime is over, which might be too late

			l.cancelFunc()              // cancel any previous watcher
			ctx := context.Background() // only used to cancel the goroutine in memlimit watcher
			ctx, cancelFunc := context.WithCancel(ctx)
			l.cancelFunc = cancelFunc

			enterrors.GoWrapper(
				func() {
					ticker := time.NewTicker(time.Minute)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							l.applyCgroupLimit(conf)
						case <-ctx.Done():
							return
						}
					}
				}, l.logger)
		} else if memLimit := conf.GoMemLimit.Get(); memLimit != "" {
			limitBytes, err := parseMemLimit(memLimit)
			if err != nil {
				return err
			}
			debug.SetMemoryLimit(limitBytes)
		}

		// GOMAXPROCS of <=0 means "do not change the current settings"
		if conf.GoMaxProcs.Get() > 0 {
			goruntime.GOMAXPROCS(conf.GoMaxProcs.Get())
		}
		return nil
	}
}

func (l *Limits) applyCgroupLimit(conf config.ResourceLimits) {
	limit, err := memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(conf.GoMemLimitFromCgroupsRatio.Get()),
		memlimit.WithProvider(memlimit.FromCgroup),
	)
	if err != nil {
		l.logger.Warn("unable to set memory limit from cgroups: %w", err)
		return
	}
	if limit == 0 {
		l.logger.Warn("limit not set. Is GOMEMLIMIT set in the environment?")
		return

	}
	if limit == l.previousLimit {
		return
	}
	l.logger.
		WithField("new_limit", strconv.FormatInt(limit, 10)).
		WithField("previous_limit", strconv.FormatInt(l.previousLimit, 10)).
		Info("updated GOMEMLIMIT from cgroups")
	l.previousLimit = limit
}

func limitResourcesDeprecated(logger logrus.FieldLogger) {
	logger.Info("Limiting resources:  memory: 80%, cores: all but one")
	if os.Getenv("GOMAXPROCS") == "" {
		// Fetch the number of cores from the cgroups cpuset
		// and parse it into an int
		cores, err := getCores()
		if err == nil {
			logger.WithField("cores", cores).
				Warn("GOMAXPROCS not set, and unable to read from cgroups, setting to number of cores")
			goruntime.GOMAXPROCS(cores)
		} else {
			cores = goruntime.NumCPU() - 1
			if cores > 0 {
				logger.WithField("cores", cores).
					Warnf("Unable to read from cgroups: %v, setting to max cores to: %v", err, cores)
				goruntime.GOMAXPROCS(cores)
			}
		}
	}

	limit, err := memlimit.SetGoMemLimit(0.8)
	if err != nil {
		logger.Warnf("Unable to set memory limit from cgroups: %v", err)
		// Set memory limit to 90% of the available memory
		limit := int64(float64(memory.TotalMemory()) * 0.8)
		debug.SetMemoryLimit(limit)
		logger.WithField("limit", limit).Info("Set memory limit based on available memory")
	} else {
		logger.WithField("limit", limit).Info("Set memory limit")
	}
}

func getCores() (int, error) {
	cpuset, err := os.ReadFile("/sys/fs/cgroup/cpuset/cpuset.cpus")
	if err != nil {
		return 0, errors.Wrap(err, "read cpuset")
	}
	return calcCPUs(strings.TrimSpace(string(cpuset)))
}

func calcCPUs(cpuString string) (int, error) {
	cores := 0
	if cpuString == "" {
		return 0, nil
	}

	// Split by comma to handle multiple ranges
	ranges := strings.Split(cpuString, ",")
	for _, r := range ranges {
		// Check if it's a range (contains a hyphen)
		if strings.Contains(r, "-") {
			parts := strings.Split(r, "-")
			if len(parts) != 2 {
				return 0, fmt.Errorf("invalid CPU range format: %s", r)
			}
			start, err := strconv.Atoi(parts[0])
			if err != nil {
				return 0, fmt.Errorf("invalid start of CPU range: %s", parts[0])
			}
			end, err := strconv.Atoi(parts[1])
			if err != nil {
				return 0, fmt.Errorf("invalid end of CPU range: %s", parts[1])
			}
			cores += end - start + 1
		} else {
			// Single CPU
			cores++
		}
	}

	return cores, nil
}

func parseMemLimit(goMemLimit string) (int64, error) {
	val, err := func() (int64, error) {
		switch {
		case strings.HasSuffix(goMemLimit, "GiB"):
			limit, err := strconv.ParseInt(strings.TrimSuffix(goMemLimit, "GiB"), 10, 64)
			if err != nil {
				return 0, err
			}
			return limit * 1024 * 1024 * 1024, nil
		case strings.HasSuffix(goMemLimit, "MiB"):
			limit, err := strconv.ParseInt(strings.TrimSuffix(goMemLimit, "MiB"), 10, 64)
			if err != nil {
				return 0, err
			}
			return limit * 1024 * 1024, nil
		default:
			// Assume bytes if no unit is specified
			return strconv.ParseInt(goMemLimit, 10, 64)
		}
	}()
	if err != nil {
		return 0, fmt.Errorf("invalid memory limit format. Acceptable values are: XXXGiB, YYYMiB or ZZZ in bytes without a unit. Got: %s: %w", goMemLimit, err)
	}
	return val, nil
}
