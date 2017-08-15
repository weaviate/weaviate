package xy_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
)

func ExampleAngle() {
	p1 := geom.Coord{-4.007890598483777E8, 7.149034067497588E8, -4.122305737303918E7}
	p2 := geom.Coord{6.452880325856061E8, -7.013452035812421E7, 6.060122721006607E8}

	angle := xy.Angle(p1, p2)
	fmt.Println(angle)
	// Output: -0.6437947786359727
}
func ExampleAngleFromOrigin() {
	p1 := geom.Coord{-643891.5406414514, 6.214131154131615E8, -9.241166163738243E7}
	angle := xy.AngleFromOrigin(p1)
	fmt.Println(angle)
	// Output: 1.571832499502282
}

func ExampleIsAcute() {
	p1 := geom.Coord{-2.9746056181996536E8, 1.283116247239797E9, 3.0124856147872955E8}
	p2 := geom.Coord{2.9337112870686615E8, -1.0822405666887188E9, 9.613329966907622E7}
	p3 := geom.Coord{-3.402935182393674E7, -8.477260955562395E8, 2.4474783489619292E7}

	isAcute := xy.IsAcute(p1, p2, p3)
	fmt.Println(isAcute)
	// Output: true
}
func ExampleIsObtuse() {
	p1 := geom.Coord{-6.581881182734076E8, -5.1226495000032324E8, 4.942792920863176E8}
	p2 := geom.Coord{-2.8760338491412956E8, -2.7637897930097174E7, -1.3120283887929991E8}
	p3 := geom.Coord{-7.253118635362322E8, 2.854840728999085E8, -3.3865131338040566E8}

	isObtuse := xy.IsObtuse(p1, p2, p3)
	fmt.Println(isObtuse)
	// Output: false
}

func ExampleAngleBetween() {
	p1 := geom.Coord{-8.6092078831365E7, -1.2832262246888882E8, -5.39892066777803E8}
	p2 := geom.Coord{-4.125610572401442E7, 3.097372706101881E8, 1.5483271373430803E8}
	p3 := geom.Coord{1.641532856745057E8, 3.949735922042323E7, 1.9570089185263705E8}

	angle := xy.AngleBetween(p1, p2, p3)
	fmt.Println(angle)
	// Output: 0.7519299818333081
}

func ExampleAngleBetweenOriented() {

	p1 := geom.Coord{-1.3799002832563987E9, 5.999590771085212E8, -4.693581090182036E8}
	p2 := geom.Coord{6.826007948791102E7, -8.657386626766933E8, -1.493830309099963E9}
	p3 := geom.Coord{-6.183224805123262E8, 2.4666014745222422E8, 7271369.117346094}

	angle := xy.AngleBetweenOriented(p1, p2, p3)
	fmt.Println(angle)
	// Output: -0.22640245255136904
}

func ExampleInteriorAngle() {
	p1 := geom.Coord{9.339625086270301E7, 9.494327011462314E8, -8.832231914445356E8}
	p2 := geom.Coord{-8.685036396637098E7, -9827198.1341636, -5.130707858094123E8}
	p3 := geom.Coord{5.48739535964397E8, 8.532792391532723E8, 2.8251807396930236E8}

	angle := xy.InteriorAngle(p1, p2, p3)
	fmt.Println(angle)
	// Output: 0.44900284899855447
}

func ExampleAngleOrientation() {
	p1 := 1.5973282539123574E8
	p2 := 1.0509666695558771E9

	orient := xy.AngleOrientation(p1, p2)
	fmt.Println(orient)
	// Output: Clockwise
}

func ExampleNormalize() {
	p1 := 7.089301226008829E8

	normalized := xy.Normalize(p1)
	fmt.Println(normalized)
	// Output: 0.7579033437162295
}

func ExampleNormalizePositive() {
	p1 := -2.269415841413788E8

	normalized := xy.NormalizePositive(p1)
	fmt.Println(normalized)
	// Output: 0.4870605702066726
}

func ExampleDiff() {

	p1 := -5.976261773911254E7
	p2 := 1.5847324519716722E8

	diff := xy.Diff(p1, p2)
	fmt.Println(diff)
	// Output: -2.1823585665309447e+08
}
