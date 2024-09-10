package compressionhelpers

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sync"
)

type SimulatedAnnealingParameters struct {
	InitTemperature  float64 // initial temperature
	TemperatureDecay float64 // temperature decay rate (0.9^(1/500))
	NIter            int     // number of iterations
	NRedo            int     // number of runs of the simulation
	Seed             int     // random seed
	Verbose          int     // verbosity level
	OnlyBitFlips     bool    // restrict permutation changes to bit flips
	InitRandom       bool    // initialize with a random permutation (not identity)
}

func NewSimulatedAnnealingParameters() SimulatedAnnealingParameters {
	return SimulatedAnnealingParameters{
		InitTemperature:  0.7,
		TemperatureDecay: 0.9997893011688015,
		NIter:            500000,
		NRedo:            2,
		Seed:             123,
		Verbose:          0,
		OnlyBitFlips:     false,
		InitRandom:       false,
	}
}

type ReproduceWithHammingObjective struct {
	N               int
	NBits           int
	DisWeightFactor float64
	TargetDis       []float64
	Weights         []float64
}

func NewReproduceWithHammingObjective(nbits int, disWeightFactor float64) ReproduceWithHammingObjective {
	n := 1 << nbits
	return ReproduceWithHammingObjective{
		N:               n,
		NBits:           nbits,
		DisWeightFactor: disWeightFactor,
		TargetDis:       make([]float64, 1),
		Weights:         make([]float64, 1),
	}
}

func (r ReproduceWithHammingObjective) DisWeight(x float64) float64 {
	return math.Exp(-r.DisWeightFactor * x)
}

func (r *ReproduceWithHammingObjective) SetAffineTargetDis(disTable []float64) {
	n := r.N
	n2 := n * n
	var sum, sum2 float64

	for i := 0; i < n2; i++ {
		sum += disTable[i]
		sum2 += disTable[i] * disTable[i]
	}

	mean := sum / float64(n2)
	stddev := math.Sqrt(sum2/float64(n2) - math.Pow(sum/float64(n2), 2))

	r.TargetDis = make([]float64, 0, n2)
	r.Weights = make([]float64, 0, n2)

	for i := 0; i < n2; i++ {
		td := (disTable[i]-mean)/stddev*(float64(r.NBits)/4.0) + float64(r.NBits)/2.0
		r.TargetDis = append(r.TargetDis, td)
		r.Weights = append(r.Weights, r.DisWeight(td))
	}
}

func (r ReproduceWithHammingObjective) ComputeCost(perm []int) float64 {
	n := r.N
	var cost float64

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			wanted := r.TargetDis[i*n+j]
			w := r.Weights[i*n+j]
			actual := float64(bits.OnesCount(uint(perm[i] ^ perm[j])))
			cost += w * Sqr(wanted-actual)
		}
	}

	return cost
}

func (r ReproduceWithHammingObjective) CostUpdate(perm []int, iw, jw int) float64 {
	n := r.N
	var deltaCost float64

	for i := 0; i < n; i++ {
		if i == iw {
			for j := 0; j < n; j++ {
				wanted := r.TargetDis[i*n+j]
				w := r.Weights[i*n+j]
				actual := float64(bits.OnesCount(uint(perm[i] ^ perm[j])))
				deltaCost -= w * Sqr(wanted-actual)
				newActual := float64(bits.OnesCount(uint(perm[jw] ^ perm[ConditionalSwap(j, iw, jw)])))
				deltaCost += w * Sqr(wanted-newActual)
			}
		} else if i == jw {
			for j := 0; j < n; j++ {
				wanted := r.TargetDis[i*n+j]
				w := r.Weights[i*n+j]
				actual := float64(bits.OnesCount(uint(perm[i] ^ perm[j])))
				deltaCost -= w * Sqr(wanted-actual)
				newActual := float64(bits.OnesCount(uint(perm[i] ^ perm[ConditionalSwap(j, iw, jw)])))
				deltaCost += w * Sqr(wanted-newActual)
			}
		} else {
			{
				j := iw
				wanted := r.TargetDis[i*n+j]
				w := r.Weights[i*n+j]
				actual := float64(bits.OnesCount(uint(perm[i] ^ perm[j])))
				deltaCost -= w * Sqr(wanted-actual)
				newActual := float64(bits.OnesCount(uint(perm[i] ^ perm[jw])))
				deltaCost += w * Sqr(wanted-newActual)
			}
			{
				j := jw
				wanted := r.TargetDis[i*n+j]
				w := r.Weights[i*n+j]
				actual := float64(bits.OnesCount(uint(perm[i] ^ perm[j])))
				deltaCost -= w * Sqr(wanted-actual)
				newActual := float64(bits.OnesCount(uint(perm[i] ^ perm[iw])))
				deltaCost += w * Sqr(wanted-newActual)
			}
		}
	}

	return deltaCost
}

func ConditionalSwap(j, iw, jw int) int {
	if j == iw {
		return jw
	} else if j == jw {
		return iw
	}
	return j
}

func Sqr(x float64) float64 {
	return x * x
}

type SimulatedAnnealingOptimizer struct {
	Obj        ReproduceWithHammingObjective
	N          int
	InitCost   float64
	Parameters SimulatedAnnealingParameters
}

func NewSimulatedAnnealingOptimizer(obj ReproduceWithHammingObjective, p SimulatedAnnealingParameters) SimulatedAnnealingOptimizer {
	return SimulatedAnnealingOptimizer{
		Obj:        obj,
		N:          obj.N,
		InitCost:   0.0,
		Parameters: p,
	}
}

func (sao *SimulatedAnnealingOptimizer) Optimize(perm []int) float64 {
	cost := sao.Obj.ComputeCost(perm)
	sao.InitCost = cost
	log2n := 0
	for sao.N > (1 << log2n) {
		log2n++
	}
	temperature := sao.Parameters.InitTemperature
	nSwap := 0
	nHot := 0
	for it := 0; it < sao.Parameters.NIter; it++ {
		temperature *= sao.Parameters.TemperatureDecay
		var iw, jw int
		if sao.Parameters.OnlyBitFlips {
			iw = rand.Intn(sao.N)
			jw = iw ^ (1 << rand.Intn(log2n))
		} else {
			iw = rand.Intn(sao.N)
			jw = rand.Intn(sao.N - 1)
			if jw >= iw {
				jw++
			}
		}
		deltaCost := sao.Obj.CostUpdate(perm, iw, jw)
		if deltaCost < 0.0 || rand.Float64() < temperature {
			perm[iw], perm[jw] = perm[jw], perm[iw]
			cost += deltaCost
			nSwap++
			if deltaCost >= 0.0 {
				nHot++
			}
		}
		if sao.Parameters.Verbose > 2 || (sao.Parameters.Verbose > 1 && it%10000 == 0) {
			fmt.Printf("      iteration %d cost %f temp %f n_swap %d (%d hot)     \r",
				it, cost, temperature, nSwap, nHot)
		}
	}
	if sao.Parameters.Verbose > 1 {
		fmt.Println("")
	}
	return cost
}

func (sao *SimulatedAnnealingOptimizer) RunOptimization(bestPerm []int, m int) float64 {
	var (
		minCost = 1e30
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	results := make(chan struct {
		perm []int
		cost float64
	}, sao.Parameters.NRedo)

	for it := 0; it < sao.Parameters.NRedo; it++ {
		wg.Add(1)

		go func(it int) {
			defer wg.Done()

			perm := make([]int, sao.N)
			for i := 0; i < sao.N; i++ {
				perm[i] = i
			}

			if sao.Parameters.InitRandom {
				for i := 0; i < sao.N; i++ {
					j := i + rand.Intn(sao.N-i)
					perm[i], perm[j] = perm[j], perm[i]
				}
			}

			// Perform the optimization and send the result
			cost := sao.Optimize(perm)
			results <- struct {
				perm []int
				cost float64
			}{perm: perm, cost: cost}
		}(it)
	}

	// Wait for all Goroutines to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process the results from the channel
	for result := range results {
		mu.Lock()
		if result.cost < minCost {
			copy(bestPerm, result.perm)
			minCost = result.cost
			fmt.Println("Segment:", m, " updated permutation, this is the new cost ", minCost)
		}
		mu.Unlock()
	}

	return minCost
}

/*func (sao *SimulatedAnnealingOptimizer) RunOptimization(bestPerm []int, m int) float64 {
	minCost := 1e30

	for it := 0; it < sao.Parameters.NRedo; it++ {
		perm := make([]int, sao.N)
		for i := 0; i < sao.N; i++ {
			perm[i] = i
		}
		if sao.Parameters.InitRandom {
			for i := 0; i < sao.N; i++ {
				j := i + rand.Intn(sao.N-i)
				perm[i], perm[j] = perm[j], perm[i]
			}
		}
		cost := sao.Optimize(perm)

		if sao.Parameters.Verbose > 1 {
			fmt.Printf("    optimization run %d: cost=%f %s\n", it, cost, func() string {
				if cost < minCost {
					return "keep"
				}
				return ""
			}())
		}
		if cost < minCost {
			copy(bestPerm, perm)
			minCost = cost
			fmt.Println("Segment:", m, " updated permutation, this is the new cost ", minCost)
		}
	}
	return minCost
}*/

func fvecL2Sqr(x, y []float32, d int) float32 {
	var res float32 = 0.0

	for i := 0; i < d; i++ {
		tmp := x[i] - y[i]
		res += tmp * tmp
	}

	return res
}

type PolysemousTraining struct {
	DisWeightFactor          float64
	SimulatedAnnealingParams SimulatedAnnealingParameters
}

func NewPolysemousTraining() PolysemousTraining {
	return PolysemousTraining{
		DisWeightFactor:          math.Log(2.0),
		SimulatedAnnealingParams: NewSimulatedAnnealingParameters(),
	}
}

func (pq *ProductQuantizer) GetMthCentroids(m int) []float32 {
	res := make([]float32, 0)
	for i := 0; i < pq.ks; i++ {
		centroid := pq.kms[m].Centroid(byte(i))
		res = append(res, centroid...)
	}

	return res
}

func (pq *ProductQuantizer) SetCentroids(centroids []float32) {
	for m := 0; m < pq.m; m++ {
		pq.kms[m].Add(centroids[m*pq.ks*pq.ds : (m+1)*pq.ks*pq.ds])
	}
}

func (pt *PolysemousTraining) OptimizePQForHamming(pq *ProductQuantizer) {
	finalCentroids := make([]float32, pq.m*pq.ks*pq.ds)
	var mu sync.Mutex     // Mutex to protect concurrent access to finalCentroids
	var wg sync.WaitGroup // WaitGroup to wait for all goroutines to finish

	// Function to optimize for a single m, to be run concurrently
	worker := func(m int) {
		defer wg.Done() // Signal that this goroutine is done
		var disTable []float64

		centroids := pq.GetMthCentroids(m)

		for i := 0; i < pq.ks; i++ {
			for j := 0; j < pq.ks; j++ {
				disTable = append(disTable, float64(fvecL2Sqr(
					centroids[i*pq.ds:(i+1)*pq.ds],
					centroids[j*pq.ds:(j+1)*pq.ds],
					pq.ds,
				)))
			}
		}

		perm := make([]int, pq.ks)

		nbits := int(math.Ceil(math.Log2(float64(pq.ks))))
		obj := NewReproduceWithHammingObjective(nbits, pt.DisWeightFactor)
		obj.SetAffineTargetDis(disTable)
		optim := NewSimulatedAnnealingOptimizer(obj, pt.SimulatedAnnealingParams)
		optim.RunOptimization(perm, m)
		centroidsCopy := append([]float32(nil), centroids...)

		for i := 0; i < pq.ks; i++ {
			copy(centroids[perm[i]*pq.ds:(perm[i]+1)*pq.ds], centroidsCopy[i*pq.ds:(i+1)*pq.ds])
		}

		// Lock before appending to finalCentroids
		mu.Lock()
		copy(finalCentroids[m*pq.ks*pq.ds:(m+1)*pq.ks*pq.ds], centroids)
		mu.Unlock()
	}
	// Launch a goroutine for each value of m
	for m := 0; m < pq.m; m++ {
		wg.Add(1)    // Increment the WaitGroup counter
		go worker(m) // Run the worker in a separate goroutine
	}
	// Wait for all workers to finish
	wg.Wait()
	// Set the final centroids
	fmt.Println("Final centroids: ", len(finalCentroids))
	pq.SetCentroids(finalCentroids)
}
