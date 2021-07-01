package sharding

import "encoding/json"

func (s *State) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func StateFromJSON(in []byte) (*State, error) {
	s := State{}

	if err := json.Unmarshal(in, &s); err != nil {
		return nil, err
	}

	return &s, nil
}
