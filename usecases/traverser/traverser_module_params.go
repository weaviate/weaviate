//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import "github.com/semi-technologies/weaviate/entities/modulecapabilities"

func ConvertFromTraverserNearTextParams(input interface{}) *modulecapabilities.NearTextParams {
	params, ok := input.(*NearTextParams)
	if ok {
		return &modulecapabilities.NearTextParams{
			Values:       params.Values,
			Limit:        params.Limit,
			MoveTo:       convertFromTraverserExploreMove(params.MoveTo),
			MoveAwayFrom: convertFromTraverserExploreMove(params.MoveAwayFrom),
			Certainty:    params.Certainty,
			Network:      params.Network,
		}
	}
	return nil
}

func convertFromTraverserExploreMove(move ExploreMove) modulecapabilities.ExploreMove {
	return modulecapabilities.ExploreMove{
		Values:  move.Values,
		Force:   move.Force,
		Objects: convertFromTraverserObjects(move.Objects),
	}
}

func convertFromTraverserObjects(objects []ObjectMove) []modulecapabilities.ObjectMove {
	if len(objects) > 0 {
		res := make([]modulecapabilities.ObjectMove, len(objects))
		for i := range objects {
			res[i] = modulecapabilities.ObjectMove{ID: objects[i].ID, Beacon: objects[i].Beacon}
		}
		return res
	}
	return nil
}

func ConvertToTraverserNearTextParams(params interface{}) *NearTextParams {
	if extractedNearTextParams, ok := params.(modulecapabilities.NearTextParams); ok {
		return &NearTextParams{
			Values:       extractedNearTextParams.Values,
			Limit:        extractedNearTextParams.Limit,
			MoveTo:       convertToTraverserExploreMove(extractedNearTextParams.MoveTo),
			MoveAwayFrom: convertToTraverserExploreMove(extractedNearTextParams.MoveAwayFrom),
			Certainty:    extractedNearTextParams.Certainty,
			Network:      extractedNearTextParams.Network,
		}
	}
	return nil
}

func convertToTraverserExploreMove(move modulecapabilities.ExploreMove) ExploreMove {
	return ExploreMove{
		Values:  move.Values,
		Force:   move.Force,
		Objects: convertToTraverserObjects(move.Objects),
	}
}

func convertToTraverserObjects(objects []modulecapabilities.ObjectMove) []ObjectMove {
	if len(objects) > 0 {
		res := make([]ObjectMove, len(objects))
		for i := range objects {
			res[i] = ObjectMove{ID: objects[i].ID, Beacon: objects[i].Beacon}
		}
		return res
	}
	return nil
}
