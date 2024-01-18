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

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeSchemaGetter struct {
	schema     schema.Schema
	shardState *sharding.State
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaGetter) CopyShardingState(class string) *sharding.State {
	return f.shardState
}

func (f *fakeSchemaGetter) ShardOwner(class, shard string) (string, error) {
	ss := f.shardState
	x, ok := ss.Physical[shard]
	if !ok {
		return "", fmt.Errorf("shard not found")
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", fmt.Errorf("owner node not found")
	}
	return x.BelongsToNodes[0], nil
}

func (f *fakeSchemaGetter) ShardReplicas(class, shard string) ([]string, error) {
	ss := f.shardState
	x, ok := ss.Physical[shard]
	if !ok {
		return nil, fmt.Errorf("shard not found")
	}
	return x.BelongsToNodes, nil
}

func (f *fakeSchemaGetter) TenantShard(class, tenant string) (string, string) {
	return tenant, models.TenantActivityStatusHOT
}

func (f *fakeSchemaGetter) ShardFromUUID(class string, uuid []byte) string {
	ss := f.shardState
	return ss.Shard("", string(uuid))
}

func (f *fakeSchemaGetter) Nodes() []string {
	return []string{"node1"}
}

func (f *fakeSchemaGetter) NodeName() string {
	return "node1"
}

func (f *fakeSchemaGetter) ClusterHealthScore() int {
	return 0
}

func (f fakeSchemaGetter) ResolveParentNodes(_ string, shard string) (map[string]string, error) {
	return nil, nil
}

func singleShardState() *sharding.State {
	config, err := sharding.ParseConfig(nil, 1)
	if err != nil {
		panic(err)
	}

	s, err := sharding.InitState("test-index", config, fakeNodes{[]string{"node1"}}, 1, false)
	if err != nil {
		panic(err)
	}

	return s
}

func multiShardState() *sharding.State {
	config, err := sharding.ParseConfig(map[string]interface{}{
		"desiredCount": json.Number("3"),
	}, 1)
	if err != nil {
		panic(err)
	}

	s, err := sharding.InitState("multi-shard-test-index", config,
		fakeNodes{[]string{"node1"}}, 1, false)
	if err != nil {
		panic(err)
	}

	return s
}

func fixedMultiShardState() *sharding.State {
	raw := []byte(`{"indexID":"multi-shard-test-index","config":{"virtualPerPhysical":128,"desiredCount":3,"actualCount":0,"desiredVirtualCount":384,"actualVirtualCount":0,"key":"_id","strategy":"hash","function":"murmur3"},"physical":{"10HAiobndwqQ":{"name":"10HAiobndwqQ","ownsVirtual":["P1ib1Jb4kcVl","cJnMLj5AELbO","OXKlgrTvR2oT","Wwn8XeDuTnEq","aFlA2RXHaTZq","oeibhOxhslJ4","xNWB5azVw7oD","wXr2DKiiO4rQ","6IcKuxi0brIT","Ib0bC21DkrSY","xLBqexoUmcoS","k7s6GojJDHw4","Ij8HLoZKUIGh","cYXwzjMjpL2p","18xE7smej6jn","wKvtkC7ISgqS","JQoplEib8uSu","CXmIRbTMCrVg","xeqIBs9YD2cA","48RZ8vWnLnYd","N2liOQZHORRd","FLYoUFplOmxQ","yPsCEegMk4Q2","MDVwNcNC9Gra","2PgGKvp3Nl9s","NKgE3HgdeGQm","NEObTyTfjbkD","MKp04TysWuOK","aV4XlXJRhzIX","jcaPi3kHStaK","8BgRFjMgSMe4","z6T8lD2o39SQ","ioHJiTM0L2b0","rblINp3n1sZ0","c5elvzxRiyRR","3W0h1o0dwd3u","JSuWUzk63Inq","2AmfpIT0zs3p","XS08CGL3mnm6","Uvrp2Des8Ab3","L215BrEJZez6","8FlXfnU8F53r","mYSXTeMJNg5X","LE68IkTZ1KBq","3DpBKBAyrh49","comyKg1nNYkV","VEUsNjgJ1M4k","nMusHd6e2Fkf","mbwwycSRznHa","UdvpOIbYGk3E","w54br5SJaUNd","glARwN18WtJG","DUFmLbD5AQdB","OyAthRrzjE9C","aRIS8UTR2sMs","dtl4DVfMpjM2","YceXk2KF4xnV","ntXhJG4ehh97","6JQHfQJXq9tC","81UYRgmxCxvy","UUvwR3z3SjqT","hVoXGtgS5V4b","pJQ37OFnd0h0","7jZlJg79Tx3C","81Tr7kFVuWFO","uoVfvn3j1vVs","si39KY4GMFCu","opTmmfmor5Xi","jUJHmkXSdjJM","taj02QCfJJ2v","vCn9VOgxbsvC","BhoBBmk2O5Fx","0Ym87WlhMWSH","8v1uPEAZANss","KcyNwToNc5uf","uTwHaNDD5s4i","nN3HYdDnNFvW","wBKHrHzpqkXI","DSeQOWKAAfPy","Dr8dRry1VNRG","qvDj32cx0hSO","FrFb8tNKNyUW","pgxcn0O1NKoW","LQdu95vAEz5N","TR2Tz7fHYqme","KoUT5JzcEigR","EAnJxQirsjKw","R8oWsRMA8Z9G","Q8PJeKnlygDF","KpWXuBwKB3Np","BpVhOJVXrM9b","zPbNJqGLV31c","nXTYGKAWzMD3","DfDu4yl3r1qn","VExbdXg0Ek0R","KISjTsevB5iE","3qa8pPUU95kc","iWYrenDncRMZ","Cqju2FPfqDzN","X4f5TfQ2Zh3w","Ol4Jrf6D4hI8","sTxMnH5hC27G","f1r8QwYMUnF2","M8Xxly7aXuVg","JprYyU27D3SD","1pZrOBR3Mp0n","7dAlRglk4gUq","meZAtRpcJSGj","RAKQqUg9tkVy","evXAOdPjRLLO","ZhRSfF5kiST2","K2KoDH0smQno","97FKS8uAgBGZ","A6uDhUMxgGzC","JcvKnGUxrjbi","7oYBFzAQhylH","0qi2orWv2yBQ","HruSBzNO6D2X","Pw1ZyGgzqr4w","Ua0wIIuK7MVF","0yiZLUGHfCfg","aTgCTGMPZTPv","04uFLfQjFQ5M","MaWgxbZRgif0","SPWsrRGsxBtQ","CMG5X2zuOP8T","rjKzLkrjnadI","3zdVkbUS6vRy"],"ownsPercentage":0.3115328681081298},"5UnDHhImGGus":{"name":"5UnDHhImGGus","ownsVirtual":["kk3uy33qESSP","AYNqxnC70vWf","1L72JpPhK3AH","N5YYIOLBJsT5","Kdpf7oo9rHRd","2zc4yEUgcsxU","04flFfZJX9y4","siSzkkL8ch9H","vSuf6j765jqp","PT37knJ3yCVo","IfrQL0OScc3u","CGR9oTerTPfV","saP105LXdjTN","wJbYdeQm8NxO","tHpmYN4knZgr","7KyVTpba2SB8","cghDTFg7bkKV","9SKvNyPOO16k","mpyKh0Uvq9pK","RBxCMlncsWP4","wzCL9BumCknu","SliMd6ggZIIT","MRj5CdiSSHkh","A9NHb9n0ZxAu","o0I69xAYCy8l","cOB2G6rQQDMp","fq4nrOTRJSFl","MVHggjKS2WIh","f7KnlZipyMoU","kC8TDVP2qpDU","RSCYtxYOBl4u","FRl5xH4a7yhi","Pn8nYsbPoQXd","VawtjdDTNeP4","xWK39CVQOq7c","IiRbFljPDBMc","DInND1HhGkNp","odhRcafbqKku","wMSpj5IJ6Jr2","urILYmbzLgFM","f8XCDnms7AuS","85iBn44HwzLG","4sh42JIRZ5x4","RlZ9POMVYbei","9DuGADJSRpBx","zY4rPaerd1ys","PA9vlkamSA8b","wgvpTSabR2Gp","lUb5dLRn4Q7i","CIohP98pYrEU","uydQQfk9x5dv","U0547wfcqSsu","OejkNdX5mxtz","ndz1d2vCwoIq","uImQxXnTgMUt","73XdZtGlJTnb","jTVVZexNxBtp","w0MlBKADJImt","0s0krALwyqS4","tq85nEKxsXd6","itKBOdSzdcD2","T1LLWfgdGEtq","vBUqaK2WYdna","t0KeI7GeoLhD","rohnBjf5J9D8","SQ4Xh9ZRBYnc","VhJWJyF49q7N","hsdU1XjrtTL0","EgXtER7wworu","xp4mWs2aVMbB","hK5JiEF1Odsf","vxlhQDXcCDoc","Ytl1gLqLrmUF","7JIKqhHrk8gs","fnaBlG1IB2tO","UtdVz5Nl280P","S2fMEGZ6FK5b","HYBYL0oNGwfa","jPIF7y5qI3fh","H6BtD1FRUcEb","PeiLrsaz0A1Z","58AEK168LCOI","b2T01LA7C14D","ofCP6twUePhY","gLBhIXTrzmM5","ZdNfpA9QwBPW","I6lAuYCU2JkE","6BoSzEVYohm2","iH2Nxldim4CZ","WCrjXqYEOGO3","3CBHlZozw0Z1","KnwHXiKEEMD5","u2NQ8vav4GGA","AJexcI7np4a6","xY2JLJ1fxlZP","aGQsUOxlSSI7","6mcznERQIGgE","WqDu1P0hsKi2","vQ8Nr3MMIvqX","cCuxaZc2WPTR","e2bHBdxkqCWf","AVApjgCPtUh6","h03hg1MFr8UK","bd7afitI4wfb","mA1JyNKe3vCB","94dJ1MbkYawY","rkAx8oOGiGp5","5dkf0GDkoFS3","ULSfcu1j23rT","IXPatqSYHjfd","Od8MgXRVCkCj","qJn82gKq9WyS","0suDv1qp3ndv","ptSPQhrhvHUx","bFNikojhtkrf","EVmTAbpjGR6I","wAmFbdhLWtEt","3XGWS3W2ZWY1","JPenxeL41DVm","WYWMO6T7W33P","gzKXC2EEuP0y","wrXomqZRYWlP","3xb5qWUIdy3l","q6uOaqi6vhFp","1tkGefsVhdMZ","VxASALmWn2ch","hiizguisdpTg","tfavKaxeSt8e"],"ownsPercentage":0.34078263030984246},"qc4oYcGtIr3y":{"name":"qc4oYcGtIr3y","ownsVirtual":["ZpSrDlljAmhU","PoixZZemlqGZ","uq0LzAAY5iuw","hDhqptRj4wek","f2c8q7U2y0Wc","zO4HfORWFYQx","E4OaMLfzLI3l","qjwhfQLQXRtk","2iX5gmw2cSrS","uMkCwD02ZjTb","UixcolZ7TGh7","PzFLgH6tNojh","a0D06UHaaNTQ","EfHluzACLDad","zLp00SBesBQM","MWsu1YNw7GyZ","EcEpTU5Z1N2m","FVhhe4CrETSN","oAGIG611zPBS","93O50DIDBjvT","psuAMG40uLmP","4ZKvAdv7y3W3","2hY6C71VJZa6","TsFT7FF0NLc9","mdW6a1iyMMQB","Pqolehn1Kgzx","6lS4e7DEBTSK","cTPggh41RFbY","IeTvnejJYwTE","shwsWnArjWTe","NPiIKetDYl31","fgTWGQMwfW8m","AmEGOYK67uL5","3KocyAIgLFK9","NtPJZ0DCROIt","qPo0bL1DanzJ","S7pN5St5UtGx","WmgtIhiclMxv","iZr9zB8YgfBS","Ec4HWsyJ98Pj","Vbw2kZ8Pzb10","W1p8DeaOGQwa","tf20ZJKNTYuN","VYlWIOMzTG6Q","E7qcPMaKvpuY","HnNcLsgB4Qle","9UENJD0jVfQJ","QEzoqtGH5L3q","c3Fmgjy0kiGM","nPdFzMMAl8e1","vPsB4hMU3u1Q","zcHMGe7BCirf","jlzvsigHmfto","6t35oHrP3E89","Un0x4I8BmBxf","ENBKLdnBVJMj","PPBVWwBeAt88","6OVVNvUPztYL","4mW6gvrMBHur","lGwgDbeyOBH7","nQF4tc5aCEj6","YhUygFdgZypN","bYeSTo07NtUQ","8Di0Ol5egcop","k0raV8wB89r3","YZq35jzzcmLB","IFXmevuG7Zrh","3C5qZO15G38a","2RvYTa9iW6Om","Xgg7Xi7IPaxF","KoRSyfj9NHxw","ngQi7Ikjaotp","pZxtjHxpnAP7","gbdKhYN535Uf","Xaqm44zLtfaI","qT0JE0lB5nbG","iHFJaeDAyc3y","VQjkAf5kPk7D","6DXk6xSSav8y","M3kWMrkyMrGT","XZG9YcJJQOQB","6MSMwtkaDKYl","Nk6OCjX40weL","uLUgLpz1cxlj","PquK03kCX2s6","gtoqy7jnEc10","rX2vYIXPVa18","8irbpjVkihDh","OH4vVMfkcJJp","tcrH84kQAesz","qhFDFwxgRnVT","IiO65JAUXrTl","g7Up5jauHf3w","aOm3ikcxu3yr","s4qjXxgUNJsT","gtk7Xsi7whtY","qcgtLYQ2qEet","HKmvdMeJ7ZUM","wkkemoIF2SPl","jv8aLeLUQAs3","XqtbcXDoer5y","6QrhJx5gbnzD","a6g520corlxT","UXRmLgtMDrXc","10sk2R5hdJ3w","7dRVmMSfU9TF","BVUpofeiRXJ0","Fbu1Q7R1XE6Q","KhlfU1eDGz8J","asfBwHMBj3DG","h10KwfryF0Hs","gpqm1I3ZXfFw","Jrt8yXZwyfFi","l7bTeARR9sxT","SF2wURCU9LAf","TEVjUfLfnSxk","WadYBzIWkqkJ","MPs72GrXF80t","XIK3cSdZyVhk","5G1XAnbIxrsI","THznNzcBVet8","scRBGW6E4EA4","maRmPi8FWELR","LKWfqTsgLJ2h","ThChEv0oUVF8","BwQaadmEuOWL","xQhsuMpqsB9Y","90E1fTakTcmi"],"ownsPercentage":0.3476845015820278}},"virtual":[{"name":"wMSpj5IJ6Jr2","upper":890026480952456,"ownsPercentage":0.0009179861551301902,"assignedToPhysical":"5UnDHhImGGus"},{"name":"DInND1HhGkNp","upper":20777246139474581,"ownsPercentage":0.001078088337923306,"assignedToPhysical":"5UnDHhImGGus"},{"name":"c5elvzxRiyRR","upper":160478001236453307,"ownsPercentage":0.007573193108700487,"assignedToPhysical":"10HAiobndwqQ"},{"name":"aOm3ikcxu3yr","upper":169380448435422008,"ownsPercentage":0.00048260262967796797,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"gtoqy7jnEc10","upper":196373783656105797,"ownsPercentage":0.0014633116344447423,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"3qa8pPUU95kc","upper":212860879729245124,"ownsPercentage":0.0008937672690237444,"assignedToPhysical":"10HAiobndwqQ"},{"name":"wrXomqZRYWlP","upper":214005801208299024,"ownsPercentage":0.00006206631774577777,"assignedToPhysical":"5UnDHhImGGus"},{"name":"psuAMG40uLmP","upper":251004648148060861,"ownsPercentage":0.002005711511577422,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"BVUpofeiRXJ0","upper":299839797586403218,"ownsPercentage":0.0026473587557352524,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"PoixZZemlqGZ","upper":312733734053195943,"ownsPercentage":0.000698981696459337,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"SPWsrRGsxBtQ","upper":332381381418126825,"ownsPercentage":0.0010651010978643578,"assignedToPhysical":"10HAiobndwqQ"},{"name":"kk3uy33qESSP","upper":348216885392409783,"ownsPercentage":0.0008584443905660211,"assignedToPhysical":"5UnDHhImGGus"},{"name":"qT0JE0lB5nbG","upper":454179755158415031,"ownsPercentage":0.005744258680155073,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"1pZrOBR3Mp0n","upper":493292167589083547,"ownsPercentage":0.0021202881264239925,"assignedToPhysical":"10HAiobndwqQ"},{"name":"RSCYtxYOBl4u","upper":560609404341315928,"ownsPercentage":0.0036492747166245697,"assignedToPhysical":"5UnDHhImGGus"},{"name":"HruSBzNO6D2X","upper":577899831252128784,"ownsPercentage":0.0009373159209952564,"assignedToPhysical":"10HAiobndwqQ"},{"name":"ENBKLdnBVJMj","upper":638066638177577744,"ownsPercentage":0.003261649139004383,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"2RvYTa9iW6Om","upper":685076307063247990,"ownsPercentage":0.0025483992566833953,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"UXRmLgtMDrXc","upper":698279650281856233,"ownsPercentage":0.0007157546700843405,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"PA9vlkamSA8b","upper":723564314835311292,"ownsPercentage":0.0013706844119711601,"assignedToPhysical":"5UnDHhImGGus"},{"name":"7JIKqhHrk8gs","upper":806465637273563321,"ownsPercentage":0.004494089694473707,"assignedToPhysical":"5UnDHhImGGus"},{"name":"b2T01LA7C14D","upper":833505543382924548,"ownsPercentage":0.0014658362473786754,"assignedToPhysical":"5UnDHhImGGus"},{"name":"odhRcafbqKku","upper":859042461960569242,"ownsPercentage":0.0013843591300233909,"assignedToPhysical":"5UnDHhImGGus"},{"name":"73XdZtGlJTnb","upper":909598291510966844,"ownsPercentage":0.002740637011517397,"assignedToPhysical":"5UnDHhImGGus"},{"name":"U0547wfcqSsu","upper":925074112165663730,"ownsPercentage":0.0008389459187409203,"assignedToPhysical":"5UnDHhImGGus"},{"name":"oAGIG611zPBS","upper":960831498707513441,"ownsPercentage":0.0019384118085538698,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"aFlA2RXHaTZq","upper":986327436288144592,"ownsPercentage":0.0013821375457237555,"assignedToPhysical":"10HAiobndwqQ"},{"name":"nQF4tc5aCEj6","upper":1025597420805531024,"ownsPercentage":0.0021288301263611246,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Xgg7Xi7IPaxF","upper":1085798861384560310,"ownsPercentage":0.003263526633127028,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"2zc4yEUgcsxU","upper":1157579081722514043,"ownsPercentage":0.0038912135415948815,"assignedToPhysical":"5UnDHhImGGus"},{"name":"WYWMO6T7W33P","upper":1203198582761505852,"ownsPercentage":0.0024730381067089822,"assignedToPhysical":"5UnDHhImGGus"},{"name":"rjKzLkrjnadI","upper":1270538940193839560,"ownsPercentage":0.00365052809120433,"assignedToPhysical":"10HAiobndwqQ"},{"name":"shwsWnArjWTe","upper":1370658063932621614,"ownsPercentage":0.005427468573246627,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Un0x4I8BmBxf","upper":1399326713180590975,"ownsPercentage":0.0015541305898436652,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"MaWgxbZRgif0","upper":1403710789048654944,"ownsPercentage":0.00023766122902481144,"assignedToPhysical":"10HAiobndwqQ"},{"name":"si39KY4GMFCu","upper":1452671883616463620,"ownsPercentage":0.0026541862548843196,"assignedToPhysical":"10HAiobndwqQ"},{"name":"EcEpTU5Z1N2m","upper":1457938633566877424,"ownsPercentage":0.00028551108690882845,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"MKp04TysWuOK","upper":1489663018037007097,"ownsPercentage":0.0017197823281639994,"assignedToPhysical":"10HAiobndwqQ"},{"name":"5G1XAnbIxrsI","upper":1500937357544703297,"ownsPercentage":0.0006111831693791686,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"A6uDhUMxgGzC","upper":1534480789179842443,"ownsPercentage":0.0018183930725718428,"assignedToPhysical":"10HAiobndwqQ"},{"name":"R8oWsRMA8Z9G","upper":1636736621003898917,"ownsPercentage":0.005543299750647721,"assignedToPhysical":"10HAiobndwqQ"},{"name":"6mcznERQIGgE","upper":1672314219256841825,"ownsPercentage":0.0019286654658828594,"assignedToPhysical":"5UnDHhImGGus"},{"name":"iHFJaeDAyc3y","upper":1824597043576342080,"ownsPercentage":0.00825526844797153,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"jv8aLeLUQAs3","upper":1845224475960403555,"ownsPercentage":0.001118215350179865,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"uoVfvn3j1vVs","upper":1849612883575629432,"ownsPercentage":0.00023789605350899137,"assignedToPhysical":"10HAiobndwqQ"},{"name":"fnaBlG1IB2tO","upper":2005466986221275098,"ownsPercentage":0.008448867833959392,"assignedToPhysical":"5UnDHhImGGus"},{"name":"Ec4HWsyJ98Pj","upper":2032295005301287389,"ownsPercentage":0.0014543498285015945,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"iZr9zB8YgfBS","upper":2141312756521615279,"ownsPercentage":0.005909864135628187,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"evXAOdPjRLLO","upper":2198349852896446041,"ownsPercentage":0.00309198719009283,"assignedToPhysical":"10HAiobndwqQ"},{"name":"BpVhOJVXrM9b","upper":2253577169221936466,"ownsPercentage":0.002993878817032044,"assignedToPhysical":"10HAiobndwqQ"},{"name":"9DuGADJSRpBx","upper":2257868273525488385,"ownsPercentage":0.0002326212304136444,"assignedToPhysical":"5UnDHhImGGus"},{"name":"asfBwHMBj3DG","upper":2290812012486862222,"ownsPercentage":0.0017858836675858434,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"hiizguisdpTg","upper":2311656255664796327,"ownsPercentage":0.0011299686868666155,"assignedToPhysical":"5UnDHhImGGus"},{"name":"Ib0bC21DkrSY","upper":2330298822826987746,"ownsPercentage":0.0010106155908977431,"assignedToPhysical":"10HAiobndwqQ"},{"name":"I6lAuYCU2JkE","upper":2364963278411676435,"ownsPercentage":0.0018791639026473376,"assignedToPhysical":"5UnDHhImGGus"},{"name":"uLUgLpz1cxlj","upper":2368186543570048126,"ownsPercentage":0.00017473355436017104,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"MVHggjKS2WIh","upper":2388232167176925479,"ownsPercentage":0.001086675433170157,"assignedToPhysical":"5UnDHhImGGus"},{"name":"qhFDFwxgRnVT","upper":2399872610948814081,"ownsPercentage":0.0006310297213088491,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"meZAtRpcJSGj","upper":2471390138583843854,"ownsPercentage":0.003876972941634569,"assignedToPhysical":"10HAiobndwqQ"},{"name":"K2KoDH0smQno","upper":2490612946772682019,"ownsPercentage":0.0010420705199805241,"assignedToPhysical":"10HAiobndwqQ"},{"name":"6OVVNvUPztYL","upper":2564700834413600126,"ownsPercentage":0.0040163124367572686,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"zLp00SBesBQM","upper":2599202103553865989,"ownsPercentage":0.001870317547769167,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"UtdVz5Nl280P","upper":2610648603275380327,"ownsPercentage":0.0006205159932710283,"assignedToPhysical":"5UnDHhImGGus"},{"name":"4ZKvAdv7y3W3","upper":2766853347457801461,"ownsPercentage":0.008467876149756172,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"HYBYL0oNGwfa","upper":2768980890630006328,"ownsPercentage":0.00011533434646806092,"assignedToPhysical":"5UnDHhImGGus"},{"name":"Kdpf7oo9rHRd","upper":2818247286096185879,"ownsPercentage":0.002670736649748094,"assignedToPhysical":"5UnDHhImGGus"},{"name":"bd7afitI4wfb","upper":2886637236964887853,"ownsPercentage":0.0037074266654011797,"assignedToPhysical":"5UnDHhImGGus"},{"name":"XIK3cSdZyVhk","upper":2964061555958206534,"ownsPercentage":0.004197180742788341,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"wkkemoIF2SPl","upper":2978179529135431872,"ownsPercentage":0.0007653368594919895,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"l7bTeARR9sxT","upper":3050093338750210702,"ownsPercentage":0.0038984554308026083,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"A9NHb9n0ZxAu","upper":3055274346561046711,"ownsPercentage":0.0002808629962086384,"assignedToPhysical":"5UnDHhImGGus"},{"name":"8BgRFjMgSMe4","upper":3057496754161327476,"ownsPercentage":0.0001204769574186351,"assignedToPhysical":"10HAiobndwqQ"},{"name":"fq4nrOTRJSFl","upper":3062424065410077862,"ownsPercentage":0.0002671100780203716,"assignedToPhysical":"5UnDHhImGGus"},{"name":"uImQxXnTgMUt","upper":3219508588015719193,"ownsPercentage":0.008515569033644234,"assignedToPhysical":"5UnDHhImGGus"},{"name":"3KocyAIgLFK9","upper":3284826999312512617,"ownsPercentage":0.0035409181715642573,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"t0KeI7GeoLhD","upper":3299284401243240197,"ownsPercentage":0.0007837373290895484,"assignedToPhysical":"5UnDHhImGGus"},{"name":"Vbw2kZ8Pzb10","upper":3328043137210732710,"ownsPercentage":0.0015590142006946198,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"ZpSrDlljAmhU","upper":3351786810141955740,"ownsPercentage":0.0012871470887408638,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"UixcolZ7TGh7","upper":3412825665246073188,"ownsPercentage":0.003308922965495603,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"jUJHmkXSdjJM","upper":3467954473586604049,"ownsPercentage":0.002988538688467028,"assignedToPhysical":"10HAiobndwqQ"},{"name":"VawtjdDTNeP4","upper":3475360162886164233,"ownsPercentage":0.00040146322136679026,"assignedToPhysical":"5UnDHhImGGus"},{"name":"gtk7Xsi7whtY","upper":3528953607002574204,"ownsPercentage":0.002905306427099608,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"nMusHd6e2Fkf","upper":3574259978294764044,"ownsPercentage":0.0024560633091213558,"assignedToPhysical":"10HAiobndwqQ"},{"name":"iH2Nxldim4CZ","upper":3603054592879595105,"ownsPercentage":0.0015609591844378313,"assignedToPhysical":"5UnDHhImGGus"},{"name":"7dAlRglk4gUq","upper":3675694468025527315,"ownsPercentage":0.003937815522114775,"assignedToPhysical":"10HAiobndwqQ"},{"name":"6MSMwtkaDKYl","upper":3762304686931209074,"ownsPercentage":0.004695149374849264,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Ytl1gLqLrmUF","upper":3823342486002833461,"ownsPercentage":0.003308865717859443,"assignedToPhysical":"5UnDHhImGGus"},{"name":"8Di0Ol5egcop","upper":3881547047175440066,"ownsPercentage":0.003155275583595276,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"maRmPi8FWELR","upper":3955364129023922481,"ownsPercentage":0.0040016320253332465,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"wJbYdeQm8NxO","upper":4044968890060555644,"ownsPercentage":0.004857483829048108,"assignedToPhysical":"5UnDHhImGGus"},{"name":"aTgCTGMPZTPv","upper":4049380745841614516,"ownsPercentage":0.00023916718112583805,"assignedToPhysical":"10HAiobndwqQ"},{"name":"lGwgDbeyOBH7","upper":4111968510537073276,"ownsPercentage":0.003392889522691397,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"DSeQOWKAAfPy","upper":4204527092302830545,"ownsPercentage":0.005017610771630561,"assignedToPhysical":"10HAiobndwqQ"},{"name":"10sk2R5hdJ3w","upper":4224041044327154580,"ownsPercentage":0.0010578534589275013,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"IeTvnejJYwTE","upper":4235756052132322162,"ownsPercentage":0.0006350718456523667,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"nPdFzMMAl8e1","upper":4236420495176881018,"ownsPercentage":0.000036019529620179725,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"LKWfqTsgLJ2h","upper":4263385304921123528,"ownsPercentage":0.0014617652652683015,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"vPsB4hMU3u1Q","upper":4292354302930656194,"ownsPercentage":0.0015704125288331784,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"FRl5xH4a7yhi","upper":4322318686813382506,"ownsPercentage":0.001624372505142075,"assignedToPhysical":"5UnDHhImGGus"},{"name":"3zdVkbUS6vRy","upper":4343664613540309444,"ownsPercentage":0.0011571650065525288,"assignedToPhysical":"10HAiobndwqQ"},{"name":"FrFb8tNKNyUW","upper":4496485165952040014,"ownsPercentage":0.008284418746261658,"assignedToPhysical":"10HAiobndwqQ"},{"name":"u2NQ8vav4GGA","upper":4532019006561781396,"ownsPercentage":0.0019262933592917625,"assignedToPhysical":"5UnDHhImGGus"},{"name":"itKBOdSzdcD2","upper":4537357993340643413,"ownsPercentage":0.0002894270532256792,"assignedToPhysical":"5UnDHhImGGus"},{"name":"f2c8q7U2y0Wc","upper":4555516035749608803,"ownsPercentage":0.00098434945139421,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"EVmTAbpjGR6I","upper":4572129041729378275,"ownsPercentage":0.0009005928587390369,"assignedToPhysical":"5UnDHhImGGus"},{"name":"xNWB5azVw7oD","upper":4613011128090221427,"ownsPercentage":0.0022162223424083078,"assignedToPhysical":"10HAiobndwqQ"},{"name":"Pqolehn1Kgzx","upper":4651985171511437890,"ownsPercentage":0.0021127871273913636,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Ua0wIIuK7MVF","upper":4696630627126192084,"ownsPercentage":0.002420234998456083,"assignedToPhysical":"10HAiobndwqQ"},{"name":"3C5qZO15G38a","upper":4784907021305312069,"ownsPercentage":0.004785472917409431,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"h03hg1MFr8UK","upper":4796223604197399554,"ownsPercentage":0.0006134731878356771,"assignedToPhysical":"5UnDHhImGGus"},{"name":"THznNzcBVet8","upper":4915944216646993407,"ownsPercentage":0.006490067405457239,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"ioHJiTM0L2b0","upper":4957842579607055296,"ownsPercentage":0.0022713148072442645,"assignedToPhysical":"10HAiobndwqQ"},{"name":"aV4XlXJRhzIX","upper":4993935340105560932,"ownsPercentage":0.00195659246717394,"assignedToPhysical":"10HAiobndwqQ"},{"name":"0qi2orWv2yBQ","upper":5024894406509825712,"ownsPercentage":0.0016782943526813434,"assignedToPhysical":"10HAiobndwqQ"},{"name":"CGR9oTerTPfV","upper":5158458025247331609,"ownsPercentage":0.007240498280011476,"assignedToPhysical":"5UnDHhImGGus"},{"name":"97FKS8uAgBGZ","upper":5174323415549741130,"ownsPercentage":0.0008600645316601428,"assignedToPhysical":"10HAiobndwqQ"},{"name":"BhoBBmk2O5Fx","upper":5187434264602339602,"ownsPercentage":0.000710740551297839,"assignedToPhysical":"10HAiobndwqQ"},{"name":"nXTYGKAWzMD3","upper":5195193606611309874,"ownsPercentage":0.00042063477315918037,"assignedToPhysical":"10HAiobndwqQ"},{"name":"WqDu1P0hsKi2","upper":5224777598831476475,"ownsPercentage":0.0016037514317949445,"assignedToPhysical":"5UnDHhImGGus"},{"name":"PT37knJ3yCVo","upper":5375620358405560754,"ownsPercentage":0.00817720238169654,"assignedToPhysical":"5UnDHhImGGus"},{"name":"f8XCDnms7AuS","upper":5495283831850589643,"ownsPercentage":0.00648696989381309,"assignedToPhysical":"5UnDHhImGGus"},{"name":"7jZlJg79Tx3C","upper":5506419823422963041,"ownsPercentage":0.0006036833127773753,"assignedToPhysical":"10HAiobndwqQ"},{"name":"Uvrp2Des8Ab3","upper":5542895169334733013,"ownsPercentage":0.001977332463985063,"assignedToPhysical":"10HAiobndwqQ"},{"name":"k0raV8wB89r3","upper":5552370617218929180,"ownsPercentage":0.0005136650590659331,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"MWsu1YNw7GyZ","upper":5663161801830126264,"ownsPercentage":0.0060060021523851235,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"qjwhfQLQXRtk","upper":5698664742506073073,"ownsPercentage":0.0019246182705242756,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"opTmmfmor5Xi","upper":5721258211780544165,"ownsPercentage":0.0012247944235683026,"assignedToPhysical":"10HAiobndwqQ"},{"name":"gLBhIXTrzmM5","upper":5774022938275618813,"ownsPercentage":0.0028603815548281695,"assignedToPhysical":"5UnDHhImGGus"},{"name":"dtl4DVfMpjM2","upper":5819402277572133063,"ownsPercentage":0.0024600189125618785,"assignedToPhysical":"10HAiobndwqQ"},{"name":"3CBHlZozw0Z1","upper":5827388583406330893,"ownsPercentage":0.0004329385067785473,"assignedToPhysical":"5UnDHhImGGus"},{"name":"PzFLgH6tNojh","upper":5831627539924522089,"ownsPercentage":0.00022979429330472422,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"pZxtjHxpnAP7","upper":5872740343819337424,"ownsPercentage":0.0022287295649864648,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"81Tr7kFVuWFO","upper":5894639010637646722,"ownsPercentage":0.0011871291069473584,"assignedToPhysical":"10HAiobndwqQ"},{"name":"WadYBzIWkqkJ","upper":5920889332354674332,"ownsPercentage":0.0014230327917022375,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"9UENJD0jVfQJ","upper":5950482002594647221,"ownsPercentage":0.001604221868191287,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"cJnMLj5AELbO","upper":5962987215025360822,"ownsPercentage":0.000677908924238621,"assignedToPhysical":"10HAiobndwqQ"},{"name":"KISjTsevB5iE","upper":5986387043408926511,"ownsPercentage":0.0012685072384624944,"assignedToPhysical":"10HAiobndwqQ"},{"name":"IfrQL0OScc3u","upper":6084330577947103338,"ownsPercentage":0.005309529646360018,"assignedToPhysical":"5UnDHhImGGus"},{"name":"tfavKaxeSt8e","upper":6101649858778365729,"ownsPercentage":0.0009388800951570619,"assignedToPhysical":"5UnDHhImGGus"},{"name":"Q8PJeKnlygDF","upper":6120544210147852964,"ownsPercentage":0.0010242648401251264,"assignedToPhysical":"10HAiobndwqQ"},{"name":"1L72JpPhK3AH","upper":6139009236851798467,"ownsPercentage":0.0010009911033710284,"assignedToPhysical":"5UnDHhImGGus"},{"name":"58AEK168LCOI","upper":6203537455014676060,"ownsPercentage":0.0034980817159405236,"assignedToPhysical":"5UnDHhImGGus"},{"name":"ntXhJG4ehh97","upper":6250479863873075726,"ownsPercentage":0.002544753083298985,"assignedToPhysical":"10HAiobndwqQ"},{"name":"EfHluzACLDad","upper":6257881409784994672,"ownsPercentage":0.00040123860787268626,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Pw1ZyGgzqr4w","upper":6276168982882809802,"ownsPercentage":0.0009913713241069314,"assignedToPhysical":"10HAiobndwqQ"},{"name":"hDhqptRj4wek","upper":6281860672615602714,"ownsPercentage":0.00030854711867037577,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Nk6OCjX40weL","upper":6284740492439475368,"ownsPercentage":0.0001561153454704777,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"IiRbFljPDBMc","upper":6322365517697796882,"ownsPercentage":0.002039656706244708,"assignedToPhysical":"5UnDHhImGGus"},{"name":"81UYRgmxCxvy","upper":6374289897584535219,"ownsPercentage":0.0028148262739082167,"assignedToPhysical":"10HAiobndwqQ"},{"name":"ULSfcu1j23rT","upper":6442652778775914548,"ownsPercentage":0.003705959215253095,"assignedToPhysical":"5UnDHhImGGus"},{"name":"aGQsUOxlSSI7","upper":6628874745657227247,"ownsPercentage":0.010095113052862145,"assignedToPhysical":"5UnDHhImGGus"},{"name":"xY2JLJ1fxlZP","upper":6635203822280934241,"ownsPercentage":0.0003430999312625172,"assignedToPhysical":"5UnDHhImGGus"},{"name":"zO4HfORWFYQx","upper":6699485210543020577,"ownsPercentage":0.0034847010402069105,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"DfDu4yl3r1qn","upper":6727354121803122774,"ownsPercentage":0.001510776706650427,"assignedToPhysical":"10HAiobndwqQ"},{"name":"M3kWMrkyMrGT","upper":6740502144735913698,"ownsPercentage":0.0007127557513810577,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"PquK03kCX2s6","upper":6747123146248742074,"ownsPercentage":0.00035892521121191684,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"CMG5X2zuOP8T","upper":6980966254492504618,"ownsPercentage":0.012676660298932517,"assignedToPhysical":"10HAiobndwqQ"},{"name":"zPbNJqGLV31c","upper":6987529689123558289,"ownsPercentage":0.00035580450429774927,"assignedToPhysical":"10HAiobndwqQ"},{"name":"85iBn44HwzLG","upper":7008758635743647494,"ownsPercentage":0.001150823502253976,"assignedToPhysical":"5UnDHhImGGus"},{"name":"jTVVZexNxBtp","upper":7184765512514992474,"ownsPercentage":0.009541351908394035,"assignedToPhysical":"5UnDHhImGGus"},{"name":"scRBGW6E4EA4","upper":7288564078426028941,"ownsPercentage":0.005626931533081278,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"E7qcPMaKvpuY","upper":7317924845818590019,"ownsPercentage":0.0015916503896428141,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"VxASALmWn2ch","upper":7320272934945058874,"ownsPercentage":0.00012729016660535615,"assignedToPhysical":"5UnDHhImGGus"},{"name":"W1p8DeaOGQwa","upper":7329379115336767830,"ownsPercentage":0.0004936470281867876,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"siSzkkL8ch9H","upper":7365032735264167895,"ownsPercentage":0.0019327866091129811,"assignedToPhysical":"5UnDHhImGGus"},{"name":"h10KwfryF0Hs","upper":7437357468956214082,"ownsPercentage":0.003920731669667602,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"JSuWUzk63Inq","upper":7474332566751363072,"ownsPercentage":0.002004424067868226,"assignedToPhysical":"10HAiobndwqQ"},{"name":"nN3HYdDnNFvW","upper":7485158231506008231,"ownsPercentage":0.0005868604622793019,"assignedToPhysical":"10HAiobndwqQ"},{"name":"BwQaadmEuOWL","upper":7507090743021187083,"ownsPercentage":0.0011889638316410127,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Wwn8XeDuTnEq","upper":7528762709048575525,"ownsPercentage":0.0011748396324463297,"assignedToPhysical":"10HAiobndwqQ"},{"name":"f1r8QwYMUnF2","upper":7530763667960730121,"ownsPercentage":0.00010847219998061223,"assignedToPhysical":"10HAiobndwqQ"},{"name":"4mW6gvrMBHur","upper":7545234120228279501,"ownsPercentage":0.0007844447892662416,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"urILYmbzLgFM","upper":7773164834661528343,"ownsPercentage":0.012356148788235075,"assignedToPhysical":"5UnDHhImGGus"},{"name":"TsFT7FF0NLc9","upper":7773394258423625280,"ownsPercentage":0.000012437087064264831,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"04flFfZJX9y4","upper":7854961957136350003,"ownsPercentage":0.004421793807448961,"assignedToPhysical":"5UnDHhImGGus"},{"name":"qvDj32cx0hSO","upper":7883310802927429864,"ownsPercentage":0.0015367940097072666,"assignedToPhysical":"10HAiobndwqQ"},{"name":"xLBqexoUmcoS","upper":7905257767824779877,"ownsPercentage":0.0011897473510584994,"assignedToPhysical":"10HAiobndwqQ"},{"name":"JprYyU27D3SD","upper":7910815885169914432,"ownsPercentage":0.00030130614502621244,"assignedToPhysical":"10HAiobndwqQ"},{"name":"jlzvsigHmfto","upper":7933413828155010817,"ownsPercentage":0.0012250369439072532,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"93O50DIDBjvT","upper":7993784483485729679,"ownsPercentage":0.0032726997831969496,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"YceXk2KF4xnV","upper":7996374436592219689,"ownsPercentage":0.0001404016392346025,"assignedToPhysical":"10HAiobndwqQ"},{"name":"VYlWIOMzTG6Q","upper":8006107196738942655,"ownsPercentage":0.0005276139847678688,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"vSuf6j765jqp","upper":8017216651818234722,"ownsPercentage":0.0006022447666049291,"assignedToPhysical":"5UnDHhImGGus"},{"name":"NEObTyTfjbkD","upper":8050138084024281148,"ownsPercentage":0.0017846744159564893,"assignedToPhysical":"10HAiobndwqQ"},{"name":"ndz1d2vCwoIq","upper":8066848342316863678,"ownsPercentage":0.0009058649171805948,"assignedToPhysical":"5UnDHhImGGus"},{"name":"vQ8Nr3MMIvqX","upper":8069602844311102736,"ownsPercentage":0.00014932185231348206,"assignedToPhysical":"5UnDHhImGGus"},{"name":"uTwHaNDD5s4i","upper":8130579194898972165,"ownsPercentage":0.00330553458888029,"assignedToPhysical":"10HAiobndwqQ"},{"name":"mdW6a1iyMMQB","upper":8250566081955192821,"ownsPercentage":0.006504502180806364,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"S7pN5St5UtGx","upper":8340682427773445817,"ownsPercentage":0.004885216895630245,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"ofCP6twUePhY","upper":8447921142937480046,"ownsPercentage":0.005813422397770006,"assignedToPhysical":"5UnDHhImGGus"},{"name":"RAKQqUg9tkVy","upper":8504599523728357916,"ownsPercentage":0.0030725411793215236,"assignedToPhysical":"10HAiobndwqQ"},{"name":"6QrhJx5gbnzD","upper":8513094344746041558,"ownsPercentage":0.0004605051701124064,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"OejkNdX5mxtz","upper":8539594040314563465,"ownsPercentage":0.0014365513752797974,"assignedToPhysical":"5UnDHhImGGus"},{"name":"6lS4e7DEBTSK","upper":8575073699044782028,"ownsPercentage":0.001923356153717363,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"xp4mWs2aVMbB","upper":8602429679782843757,"ownsPercentage":0.001482970687333907,"assignedToPhysical":"5UnDHhImGGus"},{"name":"tHpmYN4knZgr","upper":8635392688536345460,"ownsPercentage":0.0017869282851102623,"assignedToPhysical":"5UnDHhImGGus"},{"name":"2AmfpIT0zs3p","upper":8670860499000032734,"ownsPercentage":0.0019227138579016923,"assignedToPhysical":"10HAiobndwqQ"},{"name":"mA1JyNKe3vCB","upper":8676513197675841415,"ownsPercentage":0.0003064334092358853,"assignedToPhysical":"5UnDHhImGGus"},{"name":"jPIF7y5qI3fh","upper":8706429952052375065,"ownsPercentage":0.0016217905044376502,"assignedToPhysical":"5UnDHhImGGus"},{"name":"gbdKhYN535Uf","upper":8799657348866550157,"ownsPercentage":0.005053867308054841,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"8FlXfnU8F53r","upper":8899248676446159866,"ownsPercentage":0.005398856686126419,"assignedToPhysical":"10HAiobndwqQ"},{"name":"M8Xxly7aXuVg","upper":9005461565059215222,"ownsPercentage":0.005757812229011776,"assignedToPhysical":"10HAiobndwqQ"},{"name":"2hY6C71VJZa6","upper":9067556512448386258,"ownsPercentage":0.003366173842985617,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"comyKg1nNYkV","upper":9070826503298640431,"ownsPercentage":0.0001772665591926648,"assignedToPhysical":"10HAiobndwqQ"},{"name":"XS08CGL3mnm6","upper":9127646470634449826,"ownsPercentage":0.003080216601301997,"assignedToPhysical":"10HAiobndwqQ"},{"name":"CXmIRbTMCrVg","upper":9160013201600391890,"ownsPercentage":0.0017546040014764118,"assignedToPhysical":"10HAiobndwqQ"},{"name":"JQoplEib8uSu","upper":9242936734742863962,"ownsPercentage":0.004495293739162097,"assignedToPhysical":"10HAiobndwqQ"},{"name":"3XGWS3W2ZWY1","upper":9275480562589952859,"ownsPercentage":0.0017642044426404019,"assignedToPhysical":"5UnDHhImGGus"},{"name":"SliMd6ggZIIT","upper":9291129844433481316,"ownsPercentage":0.0008483492686295756,"assignedToPhysical":"5UnDHhImGGus"},{"name":"3W0h1o0dwd3u","upper":9351332456188418356,"ownsPercentage":0.0032635901227002053,"assignedToPhysical":"10HAiobndwqQ"},{"name":"z6T8lD2o39SQ","upper":9370501884911054932,"ownsPercentage":0.0010391768133194302,"assignedToPhysical":"10HAiobndwqQ"},{"name":"ptSPQhrhvHUx","upper":9557106538402328786,"ownsPercentage":0.010115858535557194,"assignedToPhysical":"5UnDHhImGGus"},{"name":"Fbu1Q7R1XE6Q","upper":9581667180131361575,"ownsPercentage":0.0013314350560127742,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"fgTWGQMwfW8m","upper":9616806305989840223,"ownsPercentage":0.0019048958297502058,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"WCrjXqYEOGO3","upper":9633001025346879568,"ownsPercentage":0.0008779174954847554,"assignedToPhysical":"5UnDHhImGGus"},{"name":"tq85nEKxsXd6","upper":9753345111991751138,"ownsPercentage":0.006523866009307677,"assignedToPhysical":"5UnDHhImGGus"},{"name":"zY4rPaerd1ys","upper":9769847995880917641,"ownsPercentage":0.0008946231282455177,"assignedToPhysical":"5UnDHhImGGus"},{"name":"OXKlgrTvR2oT","upper":9784001926533888562,"ownsPercentage":0.0007672861181580123,"assignedToPhysical":"10HAiobndwqQ"},{"name":"qJn82gKq9WyS","upper":9804919859287427473,"ownsPercentage":0.001133963406764629,"assignedToPhysical":"5UnDHhImGGus"},{"name":"AmEGOYK67uL5","upper":9837139302196143865,"ownsPercentage":0.0017466194998951496,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"cYXwzjMjpL2p","upper":9844833667801973668,"ownsPercentage":0.00041711239528692084,"assignedToPhysical":"10HAiobndwqQ"},{"name":"Ol4Jrf6D4hI8","upper":10005914080522808168,"ownsPercentage":0.008732186670839522,"assignedToPhysical":"10HAiobndwqQ"},{"name":"EAnJxQirsjKw","upper":10253565889542214993,"ownsPercentage":0.013425231467940306,"assignedToPhysical":"10HAiobndwqQ"},{"name":"UdvpOIbYGk3E","upper":10281661603144781072,"ownsPercentage":0.0015230716862716341,"assignedToPhysical":"10HAiobndwqQ"},{"name":"uMkCwD02ZjTb","upper":10369513924269020804,"ownsPercentage":0.004762483871039744,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"wzCL9BumCknu","upper":10436218359642224243,"ownsPercentage":0.003616054687302305,"assignedToPhysical":"5UnDHhImGGus"},{"name":"xeqIBs9YD2cA","upper":10556580804488844452,"ownsPercentage":0.006524861209418617,"assignedToPhysical":"10HAiobndwqQ"},{"name":"rohnBjf5J9D8","upper":10561798726114405432,"ownsPercentage":0.00028286409811461547,"assignedToPhysical":"5UnDHhImGGus"},{"name":"5dkf0GDkoFS3","upper":10565577061354284927,"ownsPercentage":0.0002048239637727944,"assignedToPhysical":"5UnDHhImGGus"},{"name":"1tkGefsVhdMZ","upper":10640136392647408019,"ownsPercentage":0.004041869448353525,"assignedToPhysical":"5UnDHhImGGus"},{"name":"E4OaMLfzLI3l","upper":10734061622421226437,"ownsPercentage":0.0050916969085987055,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"FLYoUFplOmxQ","upper":10799040812602198121,"ownsPercentage":0.0035225289580279127,"assignedToPhysical":"10HAiobndwqQ"},{"name":"LE68IkTZ1KBq","upper":10816649793406241553,"ownsPercentage":0.0009545847621499717,"assignedToPhysical":"10HAiobndwqQ"},{"name":"vBUqaK2WYdna","upper":10915645950426407919,"ownsPercentage":0.005366592425449025,"assignedToPhysical":"5UnDHhImGGus"},{"name":"k7s6GojJDHw4","upper":11001855601817753716,"ownsPercentage":0.004673434566385755,"assignedToPhysical":"10HAiobndwqQ"},{"name":"DUFmLbD5AQdB","upper":11008974609608993012,"ownsPercentage":0.00038592218566014386,"assignedToPhysical":"10HAiobndwqQ"},{"name":"a6g520corlxT","upper":11021271569193417168,"ownsPercentage":0.0006666195148199558,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"RBxCMlncsWP4","upper":11052047453477516654,"ownsPercentage":0.001668364030049158,"assignedToPhysical":"5UnDHhImGGus"},{"name":"90E1fTakTcmi","upper":11188766378182331430,"ownsPercentage":0.007411547759242115,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"wBKHrHzpqkXI","upper":11243260275836301483,"ownsPercentage":0.0029541201111818534,"assignedToPhysical":"10HAiobndwqQ"},{"name":"YZq35jzzcmLB","upper":11356751102912206819,"ownsPercentage":0.006152350063643664,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"6BoSzEVYohm2","upper":11467132392555628345,"ownsPercentage":0.005983781701657466,"assignedToPhysical":"5UnDHhImGGus"},{"name":"RlZ9POMVYbei","upper":11534987860118061834,"ownsPercentage":0.003678452267310503,"assignedToPhysical":"5UnDHhImGGus"},{"name":"mYSXTeMJNg5X","upper":11584157949447943989,"ownsPercentage":0.0026655158836382275,"assignedToPhysical":"10HAiobndwqQ"},{"name":"a0D06UHaaNTQ","upper":11686754521353283792,"ownsPercentage":0.0055617713074667344,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"uydQQfk9x5dv","upper":11719597803869881397,"ownsPercentage":0.0017804379128025154,"assignedToPhysical":"5UnDHhImGGus"},{"name":"WmgtIhiclMxv","upper":11783499227615313778,"ownsPercentage":0.0034641031224857295,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"yPsCEegMk4Q2","upper":11785036598019323645,"ownsPercentage":0.00008334101659712077,"assignedToPhysical":"10HAiobndwqQ"},{"name":"hVoXGtgS5V4b","upper":11822411649275226527,"ownsPercentage":0.002026105588420349,"assignedToPhysical":"10HAiobndwqQ"},{"name":"rX2vYIXPVa18","upper":11839341004889018262,"ownsPercentage":0.0009177422067626335,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"VEUsNjgJ1M4k","upper":11891663786092253089,"ownsPercentage":0.0028364236525515456,"assignedToPhysical":"10HAiobndwqQ"},{"name":"7KyVTpba2SB8","upper":12025520187007287877,"ownsPercentage":0.007256370033658569,"assignedToPhysical":"5UnDHhImGGus"},{"name":"0yiZLUGHfCfg","upper":12054389308461989689,"ownsPercentage":0.0015649982099467795,"assignedToPhysical":"10HAiobndwqQ"},{"name":"w54br5SJaUNd","upper":12070914180643214570,"ownsPercentage":0.0008958151159464646,"assignedToPhysical":"10HAiobndwqQ"},{"name":"KhlfU1eDGz8J","upper":12112182337465455065,"ownsPercentage":0.002237151264057282,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"w0MlBKADJImt","upper":12241122797128861048,"ownsPercentage":0.006989876324417216,"assignedToPhysical":"5UnDHhImGGus"},{"name":"TR2Tz7fHYqme","upper":12287552841407224373,"ownsPercentage":0.0025169777437599843,"assignedToPhysical":"10HAiobndwqQ"},{"name":"jcaPi3kHStaK","upper":12488886731702542352,"ownsPercentage":0.010914332062657099,"assignedToPhysical":"10HAiobndwqQ"},{"name":"L215BrEJZez6","upper":12528467364563959494,"ownsPercentage":0.0021456704068349808,"assignedToPhysical":"10HAiobndwqQ"},{"name":"SF2wURCU9LAf","upper":12560487892291319550,"ownsPercentage":0.0017358362863068052,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"VhJWJyF49q7N","upper":12605819633176064544,"ownsPercentage":0.002457438597489524,"assignedToPhysical":"5UnDHhImGGus"},{"name":"8irbpjVkihDh","upper":12607449777738158082,"ownsPercentage":0.00008837031378436226,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"vxlhQDXcCDoc","upper":12623878231076967528,"ownsPercentage":0.000890588240025697,"assignedToPhysical":"5UnDHhImGGus"},{"name":"tf20ZJKNTYuN","upper":12632610065963450389,"ownsPercentage":0.0004733537176854718,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"18xE7smej6jn","upper":12818842642211020687,"ownsPercentage":0.010095688187759404,"assignedToPhysical":"10HAiobndwqQ"},{"name":"Xaqm44zLtfaI","upper":12839153738477675750,"ownsPercentage":0.001101066734893482,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"AVApjgCPtUh6","upper":12939045257719913228,"ownsPercentage":0.005415130108765572,"assignedToPhysical":"5UnDHhImGGus"},{"name":"kC8TDVP2qpDU","upper":12941012840812649932,"ownsPercentage":0.00010666289318454411,"assignedToPhysical":"5UnDHhImGGus"},{"name":"lUb5dLRn4Q7i","upper":12975402347204344357,"ownsPercentage":0.0018642588770289618,"assignedToPhysical":"5UnDHhImGGus"},{"name":"VQjkAf5kPk7D","upper":13015759331929071073,"ownsPercentage":0.0021877565256756513,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"6DXk6xSSav8y","upper":13034693447105073367,"ownsPercentage":0.0010264204403956224,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"vCn9VOgxbsvC","upper":13049209090978829828,"ownsPercentage":0.0007868946311476329,"assignedToPhysical":"10HAiobndwqQ"},{"name":"0s0krALwyqS4","upper":13077149257960491748,"ownsPercentage":0.0015146394870562806,"assignedToPhysical":"5UnDHhImGGus"},{"name":"pgxcn0O1NKoW","upper":13176230253235278906,"ownsPercentage":0.0053711915164475115,"assignedToPhysical":"10HAiobndwqQ"},{"name":"gpqm1I3ZXfFw","upper":13232741920389130436,"ownsPercentage":0.0030635036149491775,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"04uFLfQjFQ5M","upper":13283819730558978214,"ownsPercentage":0.0027689336375975575,"assignedToPhysical":"10HAiobndwqQ"},{"name":"OH4vVMfkcJJp","upper":13344918619775115132,"ownsPercentage":0.0033121774212293404,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"xQhsuMpqsB9Y","upper":13434661038335714055,"ownsPercentage":0.004864946258375241,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"cTPggh41RFbY","upper":13540465127958032066,"ownsPercentage":0.00573565119131841,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"ThChEv0oUVF8","upper":13608899141929009629,"ownsPercentage":0.0037098153309618616,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"tcrH84kQAesz","upper":13751466221448629825,"ownsPercentage":0.007728576867004294,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"bFNikojhtkrf","upper":13791329741974145281,"ownsPercentage":0.0021610057778342177,"assignedToPhysical":"5UnDHhImGGus"},{"name":"AJexcI7np4a6","upper":13794193112533578183,"ownsPercentage":0.00015522362905840933,"assignedToPhysical":"5UnDHhImGGus"},{"name":"KoUT5JzcEigR","upper":13800391291566166029,"ownsPercentage":0.00033600395862929224,"assignedToPhysical":"10HAiobndwqQ"},{"name":"c3Fmgjy0kiGM","upper":13807991780268228783,"ownsPercentage":0.0004120233181363985,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"hK5JiEF1Odsf","upper":13822883361746399217,"ownsPercentage":0.0008072742495188642,"assignedToPhysical":"5UnDHhImGGus"},{"name":"bYeSTo07NtUQ","upper":14015079807354872646,"ownsPercentage":0.01041899019363495,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"aRIS8UTR2sMs","upper":14090228842807991479,"ownsPercentage":0.004073837374923082,"assignedToPhysical":"10HAiobndwqQ"},{"name":"rkAx8oOGiGp5","upper":14101487892057637593,"ownsPercentage":0.0006103542828293803,"assignedToPhysical":"5UnDHhImGGus"},{"name":"cOB2G6rQQDMp","upper":14213755249803946573,"ownsPercentage":0.006086025658387776,"assignedToPhysical":"5UnDHhImGGus"},{"name":"ngQi7Ikjaotp","upper":14228350724118087400,"ownsPercentage":0.0007912222479923932,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"sTxMnH5hC27G","upper":14241811106918739693,"ownsPercentage":0.0007296888137476867,"assignedToPhysical":"10HAiobndwqQ"},{"name":"hsdU1XjrtTL0","upper":14243251849196632867,"ownsPercentage":0.00007810279538417468,"assignedToPhysical":"5UnDHhImGGus"},{"name":"xWK39CVQOq7c","upper":14260628115736981572,"ownsPercentage":0.0009419692966366622,"assignedToPhysical":"5UnDHhImGGus"},{"name":"N2liOQZHORRd","upper":14295405224153487552,"ownsPercentage":0.0018852708248969852,"assignedToPhysical":"10HAiobndwqQ"},{"name":"gzKXC2EEuP0y","upper":14373776598404986114,"ownsPercentage":0.004248520711207463,"assignedToPhysical":"5UnDHhImGGus"},{"name":"HnNcLsgB4Qle","upper":14436355817013137488,"ownsPercentage":0.0033924262383701513,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"g7Up5jauHf3w","upper":14449061804126616310,"ownsPercentage":0.0006887929416003281,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"qPo0bL1DanzJ","upper":14466288443411053553,"ownsPercentage":0.0009338579868405498,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"e2bHBdxkqCWf","upper":14522703947576021700,"ownsPercentage":0.0030582906088761743,"assignedToPhysical":"5UnDHhImGGus"},{"name":"SQ4Xh9ZRBYnc","upper":14646673551193670540,"ownsPercentage":0.006720405678221086,"assignedToPhysical":"5UnDHhImGGus"},{"name":"zcHMGe7BCirf","upper":14679858498593792044,"ownsPercentage":0.0017989596032514463,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"wKvtkC7ISgqS","upper":14689830528749515078,"ownsPercentage":0.0005405848379462938,"assignedToPhysical":"10HAiobndwqQ"},{"name":"PeiLrsaz0A1Z","upper":14714795221021913078,"ownsPercentage":0.0013533386798582998,"assignedToPhysical":"5UnDHhImGGus"},{"name":"Dr8dRry1VNRG","upper":14775254950618810772,"ownsPercentage":0.0032775285088421317,"assignedToPhysical":"10HAiobndwqQ"},{"name":"0Ym87WlhMWSH","upper":14833826949240357903,"ownsPercentage":0.0031751944076149684,"assignedToPhysical":"10HAiobndwqQ"},{"name":"pJQ37OFnd0h0","upper":14860665055930654367,"ownsPercentage":0.0014548966789508589,"assignedToPhysical":"10HAiobndwqQ"},{"name":"3xb5qWUIdy3l","upper":14949938709884717460,"ownsPercentage":0.004839534478135717,"assignedToPhysical":"5UnDHhImGGus"},{"name":"P1ib1Jb4kcVl","upper":14974180759437663598,"ownsPercentage":0.0013141641395402726,"assignedToPhysical":"10HAiobndwqQ"},{"name":"6t35oHrP3E89","upper":15157689149190409377,"ownsPercentage":0.009948009741962183,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"IFXmevuG7Zrh","upper":15179330466362727829,"ownsPercentage":0.001173178154683776,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Ij8HLoZKUIGh","upper":15222784680299792327,"ownsPercentage":0.0023556576577107606,"assignedToPhysical":"10HAiobndwqQ"},{"name":"ZhRSfF5kiST2","upper":15269456053544292690,"ownsPercentage":0.002530060213228457,"assignedToPhysical":"10HAiobndwqQ"},{"name":"AYNqxnC70vWf","upper":15311513209867171019,"ownsPercentage":0.0022799230126913576,"assignedToPhysical":"5UnDHhImGGus"},{"name":"YhUygFdgZypN","upper":15313859598534298546,"ownsPercentage":0.0001271979845197516,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"0suDv1qp3ndv","upper":15378307640662597300,"ownsPercentage":0.003493735364396941,"assignedToPhysical":"5UnDHhImGGus"},{"name":"PPBVWwBeAt88","upper":15385273392099262806,"ownsPercentage":0.00037761414203133826,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"wXr2DKiiO4rQ","upper":15398475090070389854,"ownsPercentage":0.0007156654810396711,"assignedToPhysical":"10HAiobndwqQ"},{"name":"HKmvdMeJ7ZUM","upper":15401238888200171185,"ownsPercentage":0.00014982579683101466,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"48RZ8vWnLnYd","upper":15410079619103447657,"ownsPercentage":0.00047925698258460435,"assignedToPhysical":"10HAiobndwqQ"},{"name":"oeibhOxhslJ4","upper":15414157861976474291,"ownsPercentage":0.0002210819891429501,"assignedToPhysical":"10HAiobndwqQ"},{"name":"Cqju2FPfqDzN","upper":15424911264355003597,"ownsPercentage":0.0005829431110206132,"assignedToPhysical":"10HAiobndwqQ"},{"name":"H6BtD1FRUcEb","upper":15510388510461004250,"ownsPercentage":0.004633730796310202,"assignedToPhysical":"5UnDHhImGGus"},{"name":"94dJ1MbkYawY","upper":15520430127780613983,"ownsPercentage":0.000544357165659447,"assignedToPhysical":"5UnDHhImGGus"},{"name":"KcyNwToNc5uf","upper":15531135481528689161,"ownsPercentage":0.0005803383895444473,"assignedToPhysical":"10HAiobndwqQ"},{"name":"IXPatqSYHjfd","upper":15577751823830575066,"ownsPercentage":0.002527076979851631,"assignedToPhysical":"5UnDHhImGGus"},{"name":"Jrt8yXZwyfFi","upper":15630618935322638641,"ownsPercentage":0.0028659318566364353,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"OyAthRrzjE9C","upper":15636043160678880161,"ownsPercentage":0.00029404784576440076,"assignedToPhysical":"10HAiobndwqQ"},{"name":"LQdu95vAEz5N","upper":15694615554822090470,"ownsPercentage":0.003175215848887293,"assignedToPhysical":"10HAiobndwqQ"},{"name":"2iX5gmw2cSrS","upper":15834882014397728570,"ownsPercentage":0.0076038600099378505,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"wAmFbdhLWtEt","upper":15923976449109514542,"ownsPercentage":0.004829818983544314,"assignedToPhysical":"5UnDHhImGGus"},{"name":"JPenxeL41DVm","upper":16062655725361741270,"ownsPercentage":0.007517818629569082,"assignedToPhysical":"5UnDHhImGGus"},{"name":"wgvpTSabR2Gp","upper":16095714196141917796,"ownsPercentage":0.001792103291945798,"assignedToPhysical":"5UnDHhImGGus"},{"name":"q6uOaqi6vhFp","upper":16096660802391646118,"ownsPercentage":0.00005131562762219013,"assignedToPhysical":"5UnDHhImGGus"},{"name":"QEzoqtGH5L3q","upper":16119600637102882747,"ownsPercentage":0.0012435709315190568,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"ZdNfpA9QwBPW","upper":16184079002913190227,"ownsPercentage":0.003495379214492522,"assignedToPhysical":"5UnDHhImGGus"},{"name":"o0I69xAYCy8l","upper":16202714695155389065,"ownsPercentage":0.0010102429007381622,"assignedToPhysical":"5UnDHhImGGus"},{"name":"NtPJZ0DCROIt","upper":16335259737813065899,"ownsPercentage":0.007185281160081854,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"FVhhe4CrETSN","upper":16363533504010903771,"ownsPercentage":0.001532723936802152,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Od8MgXRVCkCj","upper":16372836885229567459,"ownsPercentage":0.0005043373064368005,"assignedToPhysical":"5UnDHhImGGus"},{"name":"iWYrenDncRMZ","upper":16399843699729802176,"ownsPercentage":0.001464042347653375,"assignedToPhysical":"10HAiobndwqQ"},{"name":"JcvKnGUxrjbi","upper":16445456059619812276,"ownsPercentage":0.0024726509842469815,"assignedToPhysical":"10HAiobndwqQ"},{"name":"qcgtLYQ2qEet","upper":16479360271815554726,"ownsPercentage":0.0018379510259516748,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"rblINp3n1sZ0","upper":16487866384862819249,"ownsPercentage":0.0004611173122625745,"assignedToPhysical":"10HAiobndwqQ"},{"name":"XZG9YcJJQOQB","upper":16521085219067707559,"ownsPercentage":0.0018007966106187846,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"7dRVmMSfU9TF","upper":16595274498779891691,"ownsPercentage":0.0040218089119542395,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"f7KnlZipyMoU","upper":16684900004595745633,"ownsPercentage":0.0048586084057830525,"assignedToPhysical":"5UnDHhImGGus"},{"name":"KoRSyfj9NHxw","upper":16691189783006387724,"ownsPercentage":0.0003409695708635289,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"NKgE3HgdeGQm","upper":16839333850387189639,"ownsPercentage":0.00803090598475522,"assignedToPhysical":"10HAiobndwqQ"},{"name":"4sh42JIRZ5x4","upper":16853708534942197307,"ownsPercentage":0.000779253211166657,"assignedToPhysical":"5UnDHhImGGus"},{"name":"N5YYIOLBJsT5","upper":16912253308197006536,"ownsPercentage":0.0031737185175267713,"assignedToPhysical":"5UnDHhImGGus"},{"name":"MDVwNcNC9Gra","upper":16913676074504809068,"ownsPercentage":0.00007712831609293425,"assignedToPhysical":"10HAiobndwqQ"},{"name":"KnwHXiKEEMD5","upper":16942230975172303110,"ownsPercentage":0.001547964266940241,"assignedToPhysical":"5UnDHhImGGus"},{"name":"6JQHfQJXq9tC","upper":16984712941502358546,"ownsPercentage":0.002302952009325108,"assignedToPhysical":"10HAiobndwqQ"},{"name":"7oYBFzAQhylH","upper":17037846590624227824,"ownsPercentage":0.0028803808905006594,"assignedToPhysical":"10HAiobndwqQ"},{"name":"6IcKuxi0brIT","upper":17068513180173499251,"ownsPercentage":0.0016624391506020674,"assignedToPhysical":"10HAiobndwqQ"},{"name":"CIohP98pYrEU","upper":17175059447197700381,"ownsPercentage":0.005775884708892977,"assignedToPhysical":"5UnDHhImGGus"},{"name":"X4f5TfQ2Zh3w","upper":17191937029992434765,"ownsPercentage":0.0009149355966177495,"assignedToPhysical":"10HAiobndwqQ"},{"name":"XqtbcXDoer5y","upper":17295841694861764475,"ownsPercentage":0.005632683169135277,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"mpyKh0Uvq9pK","upper":17296806499840116196,"ownsPercentage":0.0000523021826776883,"assignedToPhysical":"5UnDHhImGGus"},{"name":"mbwwycSRznHa","upper":17300383715923936993,"ownsPercentage":0.00019392127247642982,"assignedToPhysical":"10HAiobndwqQ"},{"name":"glARwN18WtJG","upper":17305101748749341512,"ownsPercentage":0.0002557650719580751,"assignedToPhysical":"10HAiobndwqQ"},{"name":"taj02QCfJJ2v","upper":17404497849161967856,"ownsPercentage":0.005388273400197841,"assignedToPhysical":"10HAiobndwqQ"},{"name":"KpWXuBwKB3Np","upper":17414490882508683665,"ownsPercentage":0.0005417234232114685,"assignedToPhysical":"10HAiobndwqQ"},{"name":"s4qjXxgUNJsT","upper":17416265334764561276,"ownsPercentage":0.0000961932495397155,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"8v1uPEAZANss","upper":17467684639236524850,"ownsPercentage":0.002787446080809826,"assignedToPhysical":"10HAiobndwqQ"},{"name":"NPiIKetDYl31","upper":17513502977661685481,"ownsPercentage":0.002483817103011761,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"Pn8nYsbPoQXd","upper":17545583939057489619,"ownsPercentage":0.0017391124020377222,"assignedToPhysical":"5UnDHhImGGus"},{"name":"cCuxaZc2WPTR","upper":17551748894130738275,"ownsPercentage":0.00033420288418458625,"assignedToPhysical":"5UnDHhImGGus"},{"name":"saP105LXdjTN","upper":17666884407730275401,"ownsPercentage":0.006241508698742624,"assignedToPhysical":"5UnDHhImGGus"},{"name":"3DpBKBAyrh49","upper":17697646293978287087,"ownsPercentage":0.0016676051949923117,"assignedToPhysical":"10HAiobndwqQ"},{"name":"MPs72GrXF80t","upper":17857969619417060675,"ownsPercentage":0.008691144887040944,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"VExbdXg0Ek0R","upper":17904914075552982977,"ownsPercentage":0.0025448640664358715,"assignedToPhysical":"10HAiobndwqQ"},{"name":"TEVjUfLfnSxk","upper":17954419263366483034,"ownsPercentage":0.002683681608834984,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"uq0LzAAY5iuw","upper":18090967361863579046,"ownsPercentage":0.007402287251965807,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"S2fMEGZ6FK5b","upper":18099625189226614089,"ownsPercentage":0.000469341761800352,"assignedToPhysical":"5UnDHhImGGus"},{"name":"9SKvNyPOO16k","upper":18126877605085664304,"ownsPercentage":0.001477356423993033,"assignedToPhysical":"5UnDHhImGGus"},{"name":"UUvwR3z3SjqT","upper":18281219283787153712,"ownsPercentage":0.008366879167660726,"assignedToPhysical":"10HAiobndwqQ"},{"name":"MRj5CdiSSHkh","upper":18295367407612657153,"ownsPercentage":0.0007669713294102378,"assignedToPhysical":"5UnDHhImGGus"},{"name":"2PgGKvp3Nl9s","upper":18318403431528121668,"ownsPercentage":0.0012487853587287332,"assignedToPhysical":"10HAiobndwqQ"},{"name":"cghDTFg7bkKV","upper":18341280274520652486,"ownsPercentage":0.001240156143605585,"assignedToPhysical":"5UnDHhImGGus"},{"name":"IiO65JAUXrTl","upper":18364923769868434758,"ownsPercentage":0.0012817164510608228,"assignedToPhysical":"qc4oYcGtIr3y"},{"name":"EgXtER7wworu","upper":18388782832305629848,"ownsPercentage":0.0012934023663937104,"assignedToPhysical":"5UnDHhImGGus"},{"name":"T1LLWfgdGEtq","upper":18430700244523608817,"ownsPercentage":0.002272347469585161,"assignedToPhysical":"5UnDHhImGGus"}]}
`)

	s, err := sharding.StateFromJSON(raw, &fakeNodes{[]string{"node1"}})
	if err != nil {
		panic(err)
	}

	for name, shard := range s.Physical {
		shard.BelongsToNodes = []string{"node1"}
		s.Physical[name] = shard
	}
	return s
}

type fakeNodes struct {
	nodes []string
}

func (f fakeNodes) Candidates() []string {
	return f.nodes
}

func (f fakeNodes) LocalName() string {
	return f.nodes[0]
}

type fakeRemoteClient struct{}

func (f *fakeRemoteClient) BatchPutObjects(ctx context.Context, hostName, indexName, shardName string, objs []*storobj.Object, repl *additional.ReplicationProperties) []error {
	return nil
}

func (f *fakeRemoteClient) PutObject(ctx context.Context, hostName, indexName,
	shardName string, obj *storobj.Object,
) error {
	return nil
}

func (f *fakeRemoteClient) GetObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) Exists(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) (bool, error) {
	return false, nil
}

func (f *fakeRemoteClient) DeleteObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) error {
	return nil
}

func (f *fakeRemoteClient) MergeObject(ctx context.Context, hostName, indexName,
	shardName string, mergeDoc objects.MergeDocument,
) error {
	return nil
}

func (f *fakeRemoteClient) MultiGetObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) SearchShard(ctx context.Context, hostName, indexName,
	shardName string, vector []float32, limit int,
	filters *filters.LocalFilter, _ *searchparams.KeywordRanking, sort []filters.Sort,
	cursor *filters.Cursor, groupBy *searchparams.GroupBy, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	return nil, nil, nil
}

func (f *fakeRemoteClient) Aggregate(ctx context.Context, hostName, indexName,
	shardName string, params aggregation.Params,
) (*aggregation.Result, error) {
	return nil, nil
}

func (f *fakeRemoteClient) BatchAddReferences(ctx context.Context, hostName,
	indexName, shardName string, refs objects.BatchReferences,
) []error {
	return nil
}

func (f *fakeRemoteClient) FindUUIDs(ctx context.Context, hostName, indexName, shardName string,
	filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	return nil, nil
}

func (f *fakeRemoteClient) DeleteObjectBatch(ctx context.Context, hostName, indexName, shardName string,
	uuids []strfmt.UUID, dryRun bool,
) objects.BatchSimpleObjects {
	return nil
}

func (f *fakeRemoteClient) GetShardQueueSize(ctx context.Context,
	hostName, indexName, shardName string,
) (int64, error) {
	return 0, nil
}

func (f *fakeRemoteClient) GetShardStatus(ctx context.Context,
	hostName, indexName, shardName string,
) (string, error) {
	return "", nil
}

func (f *fakeRemoteClient) UpdateShardStatus(ctx context.Context, hostName, indexName, shardName,
	targetStatus string,
) error {
	return nil
}

func (f *fakeRemoteClient) PutFile(ctx context.Context, hostName, indexName, shardName,
	fileName string, payload io.ReadSeekCloser,
) error {
	return nil
}

type fakeNodeResolver struct{}

func (f *fakeNodeResolver) NodeHostname(string) (string, bool) {
	return "", false
}

type fakeRemoteNodeClient struct{}

func (f *fakeRemoteNodeClient) GetNodeStatus(ctx context.Context, hostName, className, output string) (*models.NodeStatus, error) {
	return &models.NodeStatus{}, nil
}

type fakeReplicationClient struct{}

func (f *fakeReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	mergeDoc *objects.MergeDocument,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, dryRun bool,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	return nil
}

func (f *fakeReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (fakeReplicationClient) Exists(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) (bool, error) {
	return false, nil
}

func (*fakeReplicationClient) FetchObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (objects.Replica, error) {
	return objects.Replica{}, nil
}

func (*fakeReplicationClient) DigestObjects(ctx context.Context,
	hostName, indexName, shardName string, ids []strfmt.UUID,
) (result []replica.RepairResponse, err error) {
	return nil, nil
}

func (*fakeReplicationClient) FetchObjects(ctx context.Context, host,
	index, shard string, ids []strfmt.UUID,
) ([]objects.Replica, error) {
	return nil, nil
}

func (*fakeReplicationClient) OverwriteObjects(ctx context.Context,
	host, index, shard string, objects []*objects.VObject,
) ([]replica.RepairResponse, error) {
	return nil, nil
}
