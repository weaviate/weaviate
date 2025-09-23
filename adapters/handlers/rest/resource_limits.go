package rest

import (
	"fmt"
	"os"
	goruntime "runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/pbnjay/memory"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/config"
)

func limitResources(appState *state.State) {

	if appState.ServerConfig.Config.ResourceLimits.Enabled && appState.ServerConfig.Config.ResourceLimits.EnabledDeprecated {
		appState.Logger.Warn("Both LIMIT_RESOURCES and LIMIT_DYNAMIC_RESOURCES_ENABLED are set to true. " +
			"LIMIT_RESOURCES is deprecated and will be ignored. Please use LIMIT_RESOURCES_DYNAMIC_ENABLED going forward.")
		appState.ServerConfig.Config.ResourceLimits.EnabledDeprecated = false
	}

	if appState.ServerConfig.Config.ResourceLimits.EnabledDeprecated {
		limitResourcesDeprecated(appState)
	} else if appState.ServerConfig.Config.ResourceLimits.Enabled {
		_ = applyResourceLimits(appState.ServerConfig.Config.ResourceLimits, appState.Logger, "startup")()
	} else {
		appState.Logger.Info("No resource limits set, weaviate will use all available memory and CPU. " +
			"To limit resources, set LIMIT_RESOURCES_DYNAMIC_ENABLED=true")
	}
}

func limitResourcesDeprecated(appState *state.State) {
	appState.Logger.Info("Limiting resources:  memory: 80%, cores: all but one")
	if os.Getenv("GOMAXPROCS") == "" {
		// Fetch the number of cores from the cgroups cpuset
		// and parse it into an int
		cores, err := getCores()
		if err == nil {
			appState.Logger.WithField("cores", cores).
				Warn("GOMAXPROCS not set, and unable to read from cgroups, setting to number of cores")
			goruntime.GOMAXPROCS(cores)
		} else {
			cores = goruntime.NumCPU() - 1
			if cores > 0 {
				appState.Logger.WithField("cores", cores).
					Warnf("Unable to read from cgroups: %v, setting to max cores to: %v", err, cores)
				goruntime.GOMAXPROCS(cores)
			}
		}
	}

	limit, err := memlimit.SetGoMemLimit(0.8)
	if err != nil {
		appState.Logger.WithError(err).Warnf("Unable to set memory limit from cgroups: %v", err)
		// Set memory limit to 90% of the available memory
		limit := int64(float64(memory.TotalMemory()) * 0.8)
		debug.SetMemoryLimit(limit)
		appState.Logger.WithField("limit", limit).Info("Set memory limit based on available memory")
	} else {
		appState.Logger.WithField("limit", limit).Info("Set memory limit")
	}

}
func parseMemLimit(goMemLimit string) (int64, error) {
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
}

func applyResourceLimits(conf config.ResourceLimits, logger logrus.FieldLogger, action string) func() error {
	return func() error {
		if memLimit := conf.GoMemLimit.Get(); memLimit != "" {
			limitBytes, err := parseMemLimit(memLimit)
			if err != nil {
				debug.SetMemoryLimit(limitBytes)
			} else {
				logger.WithField("action", action).WithError(err).Warnf("Unable to parse GOMEMLIMIT: %v", err)
			}

			logger.WithField("action", action).Info("updated GOMEMLIMIT to " + strconv.FormatInt(limitBytes, 10) + " bytes")
			debug.SetMemoryLimit(limitBytes)

		}
		if conf.GoMaxProcs.Get() > 0 {
			logger.WithField("action", action).Infof("updated GOMAXPROCS to %v cores", conf.GoMaxProcs.Get())
			goruntime.GOMAXPROCS(conf.GoMaxProcs.Get())

			fmt.Println(goruntime.GOMAXPROCS(0))
		}
		return nil
	}
}
