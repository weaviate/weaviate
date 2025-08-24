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

package oidc

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"
)

const testCertificateFilename = "certificate.crt"

func newServerWithCertificate() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf("/%s", testCertificateFilename), downloadHandler)
	s := httptest.NewServer(mux)
	return s
}

func certificateURL(s *httptest.Server) string {
	return fmt.Sprintf("%s/%s", s.URL, testCertificateFilename)
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	fileName := testCertificateFilename
	// Create in-memory buffer
	buffer := bytes.NewBufferString(testingCertificate)

	// Set headers
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileName))
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", buffer.Len()))

	// Serve file from memory
	http.ServeContent(w, r, fileName, time.Now(), bytes.NewReader(buffer.Bytes()))
}

// This certificate is only intended for tests so that certificate parsing step won't fail
// Never use these certificate for anything outside a test scenario!
var testingCertificate = `-----BEGIN CERTIFICATE-----
MIIE8zCCAtugAwIBAgIUXiJ3NER2OjKPD4f4QehH+NRsujIwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbW9jay1vaWRjMB4XDTI1MDYxMzE3MjEyNVoXDTI2MDYx
MzE3MjEyNVowFDESMBAGA1UEAwwJbW9jay1vaWRjMIICIjANBgkqhkiG9w0BAQEF
AAOCAg8AMIICCgKCAgEAvJ8n2h7h0ZKNO20MCQuUOn9eyA+VKtkkkoXfZFpsZ/vv
fvdOe9hNz+X9igJVugfwi9MgKhfPUnGOoWXJB8uwJl4yeHV8lzd5dXYkp3Pi6oCg
PkGsOwOL7KPQWkCcJDNY8Nj2IsaP+VsdQY2ydCaryztyUBZ8nKs6HWOFsMEPVQUt
01SDcWT8lBet6GZ+4QxY+JTnBCZCDoQUk9VAOWucstGfJ9lrC8PBNRNjIXtq88dR
d4e78RRdTrErwZstZgsjXvfoudNuHeAq5RdCBBuc95sCzHvcbsP7qn4JMDCtk1Xn
y7Jlh2gTAdLEjqj7yZHgmcRuDTv4AGY3jHXgB2dSYu9PXGBJR0x75O9ILfF/Rvgh
xqsaqTDmC7lFp1Tc7W0LC0joG0qsmD5Co1xhHpeivsydhI0FKcjZGp0tcKLdE8Nr
O+za2Tl8IDHkMUBNn1Spf0+MXCX5gVvWkqsvegin6+zz++0ncEqSZLJ1vhUywiJO
NnNRw8N3iYNfpuFKY3SWGOECVfw57Uzl3YD7p5HNBUSRgjIV5oPtJpluWPb9/v1X
1uYoBmerRQ48GOW/S+OeMHoTNHpGT+yFUYlooStsKUzlVg6qgDgGzciXllvSXlNP
aIk1el5LRWCQ+7NdxyIDZ7z018d5/Nvq4TGtYMxPJ1q2UslsuT8ma8kRdZDLAmcC
AwEAAaM9MDswGgYDVR0RBBMwEYIJbW9jay1vaWRjhwR/AAABMB0GA1UdDgQWBBTV
UEwK/9twnd+iP0B7eZFjR9eB3DANBgkqhkiG9w0BAQsFAAOCAgEAeAImntOcqGnN
30Y3I36v1tyYC3U109AatLarHvMglhkwmCPjStel0GuyOyxxpnzpbe17vlxSF4Bg
u/fXoM1PrPn0As/5/MQdbxZs5mDMc1D5ykpo0T+CYlgJNzxBDjCz+lODGf4vH9Ud
4ZKLWss0QCYCuF7psyOvwM223mWa63SvWgIBI7vBXcHwk5y4nVAb6jGZVTx0D9Xw
OTDsxeevpfndTuXQdSHQ09hZlmify7alAPC88Ef3oaudli8QjkNasQswAVKp+0V6
jKYEHe0XUWcdKX3kT0I1jynh7EKcR/eiHePJv9tR2Trn8bAdgnqvmUwgKPx8mOTp
tXpkz/72v3Rbbp7/mmg3U9XVIe9i7jroa05sRrrnoqW0j1EGft8MEqYw8gspnbEw
OI0tw+zwMZ0ikkZzxCn4kn+TRuzGEvUQMz32T++v7YiQwZMgRLOSTuCov0G3Co8F
LIZf11PYTnOSAmwBLDiONbBOP5OJLnvscZc9bYeEBjSUvFnDeuEb7JduvYuDbche
zGO07I7oRupj0lzXVtFJJ8D/lTjl9usQ6pFXoFEI8WaOKcUA+kFEHUfKQ9QPrJsP
MtlI4pJn5rg9W195YH0eMj06QXuy4qi/pPZQHLfx4V7UpVydWemWXeYxVpwy0j2j
9CEbs0CkTGfHn/7DvYbVqIy/QMVxsMU=
-----END CERTIFICATE-----`
