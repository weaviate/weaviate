package schema

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
)

// Diff creates human-readable information about the difference in two schemas,
// returns a len=0 slice if schemas are identical
func Diff(
	leftLabel string, left *State,
	rightLabel string, right *State,
) []string {
	var msgs []string

	if len(left.ObjectSchema.Classes) != len(right.ObjectSchema.Classes) {
		msg := fmt.Sprintf("%s has %d classes, but %s has %d classes",
			leftLabel, len(left.ObjectSchema.Classes),
			rightLabel, len(right.ObjectSchema.Classes))
		msgs = append(msgs, msg)
	}

	leftClasses := map[string]*models.Class{}
	rightClasses := map[string]*models.Class{}

	for _, class := range left.ObjectSchema.Classes {
		leftClasses[class.Class] = class
	}

	for _, class := range right.ObjectSchema.Classes {
		rightClasses[class.Class] = class
	}

	for className, classLeft := range leftClasses {
		if classRight, ok := rightClasses[className]; !ok {
			msg := fmt.Sprintf("class %s exists in %s, but not in %s",
				className, leftLabel, rightLabel)
			msgs = append(msgs, msg)
		} else {
			cc := classComparison{
				left:       classLeft,
				right:      classRight,
				leftLabel:  leftLabel,
				rightLabel: rightLabel,
			}
			msgs = append(msgs, cc.diff()...)
		}
	}

	for className := range rightClasses {
		if _, ok := leftClasses[className]; !ok {
			msg := fmt.Sprintf("class %s exists in %s, but not in %s",
				className, rightLabel, leftLabel)
			msgs = append(msgs, msg)
		}
	}

	return msgs
}

type classComparison struct {
	left, right           *models.Class
	leftLabel, rightLabel string
	msgs                  []string
}

func (cc *classComparison) addMsg(msg ...string) {
	cc.msgs = append(cc.msgs, msg...)
}

func (cc *classComparison) diff() []string {
	lj, _ := json.Marshal(cc.left)
	rj, _ := json.Marshal(cc.right)

	if bytes.Equal(lj, rj) {
		// classes are identical, we are done
		return nil
	}

	// classes are not identical, log this fact, then dig deeper to find the diff
	msg := fmt.Sprintf("class %s exists in both, but is not identical: "+
		"size %d vs %d", cc.left.Class, len(lj), len(rj))
	cc.addMsg(msg)

	pc := propsComparison{
		left:       cc.left.Properties,
		right:      cc.right.Properties,
		leftLabel:  cc.leftLabel,
		rightLabel: cc.rightLabel,
		className:  cc.left.Class,
	}
	cc.addMsg(pc.diff()...)

	ccc := classConfigComparison{
		left:       cc.left,
		right:      cc.right,
		leftLabel:  cc.leftLabel,
		rightLabel: cc.rightLabel,
		className:  cc.left.Class,
	}
	cc.addMsg(ccc.diff()...)

	return cc.msgs
}

type propsComparison struct {
	left, right           []*models.Property
	leftLabel, rightLabel string
	className             string
	msgs                  []string
}

func (cc *propsComparison) addMsg(msg ...string) {
	cc.msgs = append(cc.msgs, msg...)
}

func (pc *propsComparison) diff() []string {
	containedLeft := map[string]*models.Property{}
	containedRight := map[string]*models.Property{}

	for _, prop := range pc.left {
		containedLeft[prop.Name] = prop
	}
	for _, prop := range pc.right {
		if leftProp, ok := containedLeft[prop.Name]; !ok {
			msg := fmt.Sprintf("class %s: property %s exists in %s, but not in %s",
				pc.className, prop.Name, pc.rightLabel, pc.leftLabel)
			pc.addMsg(msg)
		} else {
			pc.compareProp(leftProp, prop)
		}
		containedRight[prop.Name] = prop
	}

	for _, prop := range pc.left {
		containedLeft[prop.Name] = prop
		if _, ok := containedRight[prop.Name]; !ok {
			msg := fmt.Sprintf("class %s: property %s exists in %s, but not in %s",
				pc.className, prop.Name, pc.leftLabel, pc.rightLabel)
			pc.addMsg(msg)
		}
	}

	return pc.msgs
}

func (pc *propsComparison) compareProp(left, right *models.Property) {
	lj, _ := json.Marshal(left)
	rj, _ := json.Marshal(right)

	if bytes.Equal(lj, rj) {
		return
	}

	msg := fmt.Sprintf("class %s: property %s: mismatch: %s has %s, but %s has %s",
		pc.className, left.Name, pc.leftLabel, lj, pc.rightLabel, rj)
	pc.addMsg(msg)
}

type classConfigComparison struct {
	left, right           *models.Class
	leftLabel, rightLabel string
	className             string
	msgs                  []string
}

func (ccc *classConfigComparison) addMsg(msg ...string) {
	ccc.msgs = append(ccc.msgs, msg...)
}

func (ccc *classConfigComparison) diff() []string {
	ccc.compare(ccc.left.Description, ccc.right.Description, "description")
	ccc.compare(ccc.left.InvertedIndexConfig,
		ccc.right.InvertedIndexConfig, "inverted index config")
	ccc.compare(ccc.left.ModuleConfig,
		ccc.right.ModuleConfig, "module config")
	ccc.compare(ccc.left.ReplicationConfig,
		ccc.right.ReplicationConfig, "replication config")
	ccc.compare(ccc.left.ShardingConfig,
		ccc.right.ShardingConfig, "sharding config")
	ccc.compare(ccc.left.VectorIndexConfig,
		ccc.right.VectorIndexConfig, "vector index config")
	ccc.compare(ccc.left.VectorIndexType,
		ccc.right.VectorIndexType, "vector index type")
	ccc.compare(ccc.left.Vectorizer,
		ccc.right.Vectorizer, "vectorizer")
	return ccc.msgs
}

func (ccc *classConfigComparison) compare(
	left, right any, label string,
) {
	lj, _ := json.Marshal(left)
	rj, _ := json.Marshal(right)

	if bytes.Equal(lj, rj) {
		return
	}

	msg := fmt.Sprintf("class %s: %s mismatch: %s has %s, but %s has %s",
		ccc.className, label, ccc.leftLabel, lj, ccc.rightLabel, rj)
	ccc.addMsg(msg)
}
