package xy_test

import (
	"testing"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
	"github.com/twpayne/go-geom/xy/orientation"
)

func TestAngle(t *testing.T) {

	for i, tc := range []struct {
		p1, p2 geom.Coord
		result float64
	}{
		{
			p1:     geom.Coord{-4.007890598483777E8, 7.149034067497588E8, -4.122305737303918E7},
			p2:     geom.Coord{6.452880325856061E8, -7.013452035812421E7, 6.060122721006607E8},
			result: -0.6437947786359727,
		},
		{
			p1:     geom.Coord{4.415559940389009E8, -1.9410956330428556E7, -3.4032011177462184E8},
			p2:     geom.Coord{-2.4046479004409158E8, -1.495553321588844E9, 3.801260331473494E8},
			result: -2.0036085354443243,
		},
		{
			p1:     geom.Coord{-4.661617106595113E8, 2.5040156355098817E8, 3.097086861435584E8},
			p2:     geom.Coord{1.3712169076859632E8, 7.234387287330664E8, 2.4094721366674533E8},
			result: 0.6649730674276739,
		},
		{
			p1:     geom.Coord{-1.7284360981816053E9, -2.361372303896285E8, -4.184641346325376E8},
			p2:     geom.Coord{-2.420562335141231E8, 6.118145442669621E7, -4.134947093381834E8},
			result: 0.19742318999485298,
		},
		{
			p1:     geom.Coord{-1.6550190854088405E8, -1.2218781891720397E9, -5.787102293280704E8},
			p2:     geom.Coord{9.876584504327146E8, 2.725822820782923E8, -7.19405957696415E8},
			result: 0.9135993912770102,
		},
	} {
		calculated := xy.Angle(tc.p1, tc.p2)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.Angle(%v, %v) expected \n\t%v but got \n\t%v", i+1, tc.p1, tc.p2, tc.result, calculated)
		}

	}
}
func TestAngleFromOrigin(t *testing.T) {

	for i, tc := range []struct {
		p1     geom.Coord
		result float64
	}{
		{
			p1:     geom.Coord{-643891.5406414514, 6.214131154131615E8, -9.241166163738243E7},
			result: 1.571832499502282,
		},
		{
			p1:     geom.Coord{-5.526240186938026E8, -4.1654756589198244E8, -3.904115882281978E8},
			result: -2.495687539636821,
		},
		{
			p1:     geom.Coord{-9775211.937969998, -7.988444321540045E8, -2.9062555922294575E8},
			result: -1.583032406419488,
		},
		{
			p1:     geom.Coord{-7.170140718747358E8, -5.5130056931151845E7, -5672967.701280272},
			result: -3.064855246212557,
		},
		{
			p1:     geom.Coord{-3.4201112699568516E8, -7.256864044916959E8, 1.5383556733916698E9},
			result: -2.0112159720228164,
		},
	} {
		calculated := xy.AngleFromOrigin(tc.p1)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.AngleFromOrigin(%v) expected \n\t%v but got \n\t%v", i+1, tc.p1, tc.result, calculated)
		}

	}
}

func TestIsAcute(t *testing.T) {

	for i, tc := range []struct {
		p1, p2, p3 geom.Coord
		result     bool
	}{
		{
			p1:     geom.Coord{-2.9746056181996536E8, 1.283116247239797E9, 3.0124856147872955E8},
			p2:     geom.Coord{2.9337112870686615E8, -1.0822405666887188E9, 9.613329966907622E7},
			p3:     geom.Coord{-3.402935182393674E7, -8.477260955562395E8, 2.4474783489619292E7},
			result: true,
		},
		{
			p1:     geom.Coord{1.2441498441622052E9, -1.9039620247337012E9, 1.3258053125928226E8},
			p2:     geom.Coord{-8.34728749413481E8, 3.979772507634378E8, 5.111888830951517E8},
			p3:     geom.Coord{6.087108620010223E8, 1.8734617987205285E8, -1.0570348250682911E8},
			result: true,
		},
		{
			p1:     geom.Coord{-5.0915064274566126E8, -1.4456369240713427E9, 2.1506319910428783E8},
			p2:     geom.Coord{6.405668498559644E8, -3.791562031465599E8, 5.596300821687293E8},
			p3:     geom.Coord{8.241172353750097E8, -3.9414469756236546E7, -2.702165842686878E8},
			result: false,
		},
		{
			p1:     geom.Coord{-1.435496848555126E9, 3.4072911256794184E7, 2.459210259260985E8},
			p2:     geom.Coord{-1.8459206790266247E9, -1.7220237003056505E9, -7.026074366858591E8},
			p3:     geom.Coord{-1.1784100863898702E9, -3.7082065759031725E8, 3577102.337059896},
			result: true,
		},
		{
			p1:     geom.Coord{1.3200492259293087E8, -1.400507993538053E9, 1.0397860978589308E8},
			p2:     geom.Coord{-1.4174043745880973E8, -2.855324865806007E8, 1.154853523604694E9},
			p3:     geom.Coord{-1.6406303327700076E9, 4.8617091926175547E8, -8.222288062865702E8},
			result: false,
		},
	} {
		calculated := xy.IsAcute(tc.p1, tc.p2, tc.p3)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.IsAcute(%v, %v, %v) expected %v but got %v", i+1, tc.p1, tc.p2, tc.p3, tc.result, calculated)
		}

	}
}
func TestIsObtuse(t *testing.T) {

	for i, tc := range []struct {
		p1, p2, p3 geom.Coord
		result     bool
	}{
		{
			p1:     geom.Coord{-6.581881182734076E8, -5.1226495000032324E8, 4.942792920863176E8},
			p2:     geom.Coord{-2.8760338491412956E8, -2.7637897930097174E7, -1.3120283887929991E8},
			p3:     geom.Coord{-7.253118635362322E8, 2.854840728999085E8, -3.3865131338040566E8},
			result: false,
		},
		{
			p1:     geom.Coord{-6.052601027752758E8, -1.0390522973193089E9, -5.487930680078092E8},
			p2:     geom.Coord{8.843340231350782E8, -8.723399162019621E8, -3321691.634961795},
			p3:     geom.Coord{-7.543599435337427E8, 1.5808204538931034E9, 1.0818796276370132E9},
			result: false,
		},
		{
			p1:     geom.Coord{-6.193065013327997E8, -6.35194942114364E7, 4.3272539543963164E7},
			p2:     geom.Coord{-5.95973359223499E8, 5945981.053576445, 9.226238629036537E8},
			p3:     geom.Coord{3.9272480109009665E8, 3.088998415162513E8, 6.645348620149242E7},
			result: true,
		},
		{
			p1:     geom.Coord{7.610654287692245E8, -6.658609134050195E8, 1.3293491844735564E8},
			p2:     geom.Coord{4.2667262625053006E8, -3.3481316736032414E8, 1.6475762301338202E8},
			p3:     geom.Coord{-4.199827597001981E8, 7.482292086773602E8, 9.971765404694296E8},
			result: true,
		},
		{
			p1:     geom.Coord{-7.643350938452588E8, -2.7699133391444945E8, 2.702299133834568E8},
			p2:     geom.Coord{-1.7382158607827853E7, 5398823.811261921, 1.5609158933203138E7},
			p3:     geom.Coord{-2.4190532792687505E8, -5.756084856732128E8, -1.460466219293458E8},
			result: false,
		},
	} {
		calculated := xy.IsObtuse(tc.p1, tc.p2, tc.p3)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.IsObtuse(%v, %v, %v) expected %v but got %v", i+1, tc.p1, tc.p2, tc.p3, tc.result, calculated)
		}

	}
}

func TestAngleBetween(t *testing.T) {

	for i, tc := range []struct {
		p1, p2, p3 geom.Coord
		result     float64
	}{
		{
			p1:     geom.Coord{-8.6092078831365E7, -1.2832262246888882E8, -5.39892066777803E8},
			p2:     geom.Coord{-4.125610572401442E7, 3.097372706101881E8, 1.5483271373430803E8},
			p3:     geom.Coord{1.641532856745057E8, 3.949735922042323E7, 1.9570089185263705E8},
			result: 0.7519299818333081,
		},
		{
			p1:     geom.Coord{-2.8151546579932548E7, -3.18057858177073E8, 4.651812237590953E8},
			p2:     geom.Coord{-3.362790282579993E8, 921376.339215076, 3.993733502580851E8},
			p3:     geom.Coord{-4.3757589855782084E7, 2.7736682744679105E8, 7.852890296262044E8},
			result: 1.5598518932475245,
		},
		{
			p1:     geom.Coord{-2.1434525095170313E8, -3.9586869555708617E8, 8.53673374777788E8},
			p2:     geom.Coord{1.6475387708561451E9, 1.3332417513595498E9, 4.7034371208287525E8},
			p3:     geom.Coord{-1.127313286995323E9, -6.606057228728307E8, -2717521.243700768},
			result: 0.1253788551617605,
		},
		{
			p1:     geom.Coord{1.0664500465302559E9, 8.475985637345538E7, -1.621500824133781E9},
			p2:     geom.Coord{4559347.7496108785, 5.161084478242324E7, -1842932.5795175508},
			p3:     geom.Coord{-4.2862346563618964E8, -8.308086105874093E8, -6.966296470909512E8},
			result: 2.058347140220315,
		},
		{
			p1:     geom.Coord{1.3687909198725855E8, -1.1203973392664804E9, -2.45804716717005E8},
			p2:     geom.Coord{-1.0056483813015188E9, 1.6751128488153452E8, -1.8167151284755492E8},
			p3:     geom.Coord{-2.228428636191809E8, -1.1812102896854641E9, 4.388310794147439E8},
			result: 0.19976523282195835,
		},
	} {
		calculated := xy.AngleBetween(tc.p1, tc.p2, tc.p3)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.AngleBetween(%v, %v, %v) expected \n\t\t%v but got \n\t\t%v", i+1, tc.p1, tc.p2, tc.p3, tc.result, calculated)
		}

	}
}

func TestAngleBetweenOriented(t *testing.T) {

	for i, tc := range []struct {
		p1, p2, p3 geom.Coord
		result     float64
	}{
		{
			p1:     geom.Coord{-1.3799002832563987E9, 5.999590771085212E8, -4.693581090182036E8},
			p2:     geom.Coord{6.826007948791102E7, -8.657386626766933E8, -1.493830309099963E9},
			p3:     geom.Coord{-6.183224805123262E8, 2.4666014745222422E8, 7271369.117346094},
			result: -0.22640245255136904,
		},
		{
			p1:     geom.Coord{6.796487221259736E7, 1.4775165450533025E9, 3258059.847120839},
			p2:     geom.Coord{-6.803421390423136E8, -1.0234495740416303E9, -5.470859926941457E8},
			p3:     geom.Coord{6.443781032426777E8, -1.810385570385187E8, 6.070318143319839E8},
			result: -0.7136563927685474,
		},
		{
			p1:     geom.Coord{5.120536476740612E7, -2.7176934954242444E8, -2.7027023064203584E8},
			p2:     geom.Coord{8.332976211782128E7, -4.67914336571098E8, -1.24317898024329E9},
			p3:     geom.Coord{-1.2179566171482772E8, -1.7824466580072454E8, -4.298802275705581E8},
			result: 0.4538277103800854,
		},
		{
			p1:     geom.Coord{-8.202691782975099E8, 8.782971263839295E8, -9.219191553882729E8},
			p2:     geom.Coord{-1.2725212616954826E8, 2.2006225859706864E8, -1.9247200296977368E8},
			p3:     geom.Coord{4.0049870580738544E8, 4.591976832016299E7, -2.1777764388295308E8},
			result: -2.7006509608972893,
		},
		{
			p1:     geom.Coord{-7.134986212152288E8, -5.527091163926333E8, 1.256171186717098E9},
			p2:     geom.Coord{7.722824262322676E7, 1.2972244051461305E8, 1.2943775785668051E8},
			p3:     geom.Coord{5.426394733747559E8, -2323555.25265493, -6.024980080960876E7},
			result: 2.1531208615200885,
		},
	} {
		calculated := xy.AngleBetweenOriented(tc.p1, tc.p2, tc.p3)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.AngleBetweenOriented(%v, %v, %v) expected \n\t%v but got \n\t%v", i+1, tc.p1, tc.p2, tc.p3, tc.result, calculated)
		}

	}
}

func TestInteriorAngle(t *testing.T) {

	for i, tc := range []struct {
		p1, p2, p3 geom.Coord
		result     float64
	}{
		{
			p1:     geom.Coord{9.339625086270301E7, 9.494327011462314E8, -8.832231914445356E8},
			p2:     geom.Coord{-8.685036396637098E7, -9827198.1341636, -5.130707858094123E8},
			p3:     geom.Coord{5.48739535964397E8, 8.532792391532723E8, 2.8251807396930236E8},
			result: 0.44900284899855447,
		},
		{
			p1:     geom.Coord{6.523521917718492E8, -1.7481105701895738E8, 1.381806851427019E9},
			p2:     geom.Coord{-8.91688057161475E7, -1.6987404322706103E9, -2.166188234151498E8},
			p3:     geom.Coord{-5.438779575706835E8, 1.7904042826669493E9, -9.194009291344139E7},
			result: 0.5824487823865407,
		},
		{
			p1:     geom.Coord{-2.115808427748782E8, 4.0164370121586424E8, -4.843953798053123E8},
			p2:     geom.Coord{2.2232659336159042E8, -1.4901190499371336E9, 4.8436342680557925E8},
			p3:     geom.Coord{7.506740282650052E8, -4.8757491165846115E8, -2.1487242670012325E7},
			result: 0.7104856314869243,
		},
		{
			p1:     geom.Coord{-1.3806701997111824E8, 4.733218140107204E7, 5.980208692031132E8},
			p2:     geom.Coord{-3.4264253869461334E8, -5.818205740522029E8, -3.6896886549013627E8},
			p3:     geom.Coord{6.63086981247813E8, -1.9734552701705813E9, -5.945945639340445E8},
			result: 2.2014190828743176,
		},
		{
			p1:     geom.Coord{1.9437329983711197E9, 1.6100156972127568E7, 8.719154732991188E8},
			p2:     geom.Coord{7.108626995403899E8, 1.3066388554032483E9, -4.715294366047639E7},
			p3:     geom.Coord{-3.4631579594085485E8, -4.448719942414226E8, 9.847856755232031E8},
			result: 1.3055971267227189,
		},
	} {
		calculated := xy.InteriorAngle(tc.p1, tc.p2, tc.p3)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.InteriorAngle(%v, %v, %v) expected \n\t%v but got \n\t%v", i+1, tc.p1, tc.p2, tc.p3, tc.result, calculated)
		}

	}
}

func TestGetAngleOrientation(t *testing.T) {

	for i, tc := range []struct {
		p1, p2 float64
		result orientation.Type
	}{
		{
			p1:     1.5973282539123574E8,
			p2:     1.0509666695558771E9,
			result: orientation.Clockwise,
		},
		{
			p1:     -1.9743974140799935E9,
			p2:     1.690220700227534E8,
			result: orientation.CounterClockwise,
		},
		{
			p1:     1.758686954900797E7,
			p2:     2.27491156028423E7,
			result: orientation.Clockwise,
		},
		{
			p1:     1.6512245510554624E8,
			p2:     3.581973387733263E8,
			result: orientation.CounterClockwise,
		},
		{
			p1:     1.1606004655250182E9,
			p2:     3.8888292684591454E8,
			result: orientation.CounterClockwise,
		},
	} {
		calculated := xy.AngleOrientation(tc.p1, tc.p2)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.GetAngleOrientation(%v, %v) expected %v but got %v", i+1, tc.p1, tc.p2, tc.result, calculated)
		}

	}
}

func TestNormalize(t *testing.T) {

	for i, tc := range []struct {
		p1     float64
		result float64
	}{
		{
			p1:     7.089301226008829E8,
			result: 0.7579033437162295,
		},
		{
			p1:     1.6423604211038163E8,
			result: -0.3600960607195205,
		},
		{
			p1:     9.606844105626652E8,
			result: 0.8766870561033144,
		},
		{
			p1:     5239293.964126772,
			result: -2.9361486826719343,
		},
		{
			p1:     6.136421257534456E8,
			result: -2.8945550816760957,
		},
	} {
		calculated := xy.Normalize(tc.p1)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.Normalize(%v) expected %v but got %v", i+1, tc.p1, tc.result, calculated)
		}

	}
}

func TestNormalizePositive(t *testing.T) {

	for i, tc := range []struct {
		p1     float64
		result float64
	}{
		{
			p1:     -2.269415841413788E8,
			result: 0.4870605702066726,
		},
		{
			p1:     4.680315524842384E7,
			result: 3.198674730205582,
		},
		{
			p1:     4.5465330578180933E8,
			result: 0.2790471976134583,
		},
		{
			p1:     4.18319606111153E7,
			result: 1.9473086960627342,
		},
		{
			p1:     -1.0427918153375134E8,
			result: 5.003804592005487,
		},
	} {
		calculated := xy.NormalizePositive(tc.p1)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.NormalizePositive(%v) expected %v but got %v", i+1, tc.p1, tc.result, calculated)
		}

	}
}

func TestDiff(t *testing.T) {

	for i, tc := range []struct {
		p1, p2 float64
		result float64
	}{
		{
			p1:     -5.976261773911254E7,
			p2:     1.5847324519716722E8,
			result: -2.1823585665309447E8,
		},
		{
			p1:     -1.019120645031252E8,
			p2:     -1.5011529441975794E9,
			result: -1.399240873411269E9,
		},
		{
			p1:     -8.346336466770616E8,
			p2:     -8.035798233809209E8,
			result: -3.1053817012955364E7,
		},
		{
			p1:     1.7851664990995303E8,
			p2:     -2.371991702990724E8,
			result: -4.1571581392584014E8,
		},
		{
			p1:     -1.855711053106832E7,
			p2:     4.015083173132894E8,
			result: -4.200654215611724E8,
		},
	} {
		calculated := xy.Diff(tc.p1, tc.p2)

		if calculated != tc.result {
			t.Errorf("Test %v failed: expected xy.Diff(%v, %v) expected %v but got %v", i+1, tc.p1, tc.p2, tc.result, calculated)
		}

	}
}
