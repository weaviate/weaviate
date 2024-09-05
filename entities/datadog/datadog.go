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

package datadog

import (
	"google.golang.org/grpc"
	grpc_datadog "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func Start() error {
	tracer.Start(
		tracer.WithService(Config.Service),
		tracer.WithEnv(Config.Environment),
		tracer.WithServiceVersion(Config.Version),
	)
	return profiler.Start(
		profiler.WithService(Config.Service),
		profiler.WithEnv(Config.Environment),
		profiler.WithVersion(Config.Version),
		profiler.WithTags(Config.Tags...),
		profiler.WithProfileTypes(withProfileTypes(Config.ProfilingAllTypes)...),
	)
}

func withProfileTypes(allTypes bool) []profiler.ProfileType {
	if allTypes {
		return []profiler.ProfileType{
			profiler.CPUProfile,
			profiler.HeapProfile,
			profiler.BlockProfile,
			profiler.MutexProfile,
			profiler.GoroutineProfile,
		}
	}
	return []profiler.ProfileType{
		profiler.CPUProfile,
		profiler.HeapProfile,
	}
}

func Stop() {
	tracer.Stop()
	profiler.Stop()
}

func GRPCTracer() grpc.UnaryServerInterceptor {
	return grpc_datadog.UnaryServerInterceptor(grpc_datadog.WithServiceName(Config.Service))
}
