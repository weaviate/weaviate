/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package telemetry

// LocalQuery contains the serviceID for a local query
const LocalQuery string = "weaviate.local.query"

// LocalQueryMeta contains the serviceID for a local meta query
const LocalQueryMeta string = "weaviate.local.query.meta"

// NetworkQuery contains the serviceID for a network query
const NetworkQuery string = "Weaviate.network.query"

// NetworkQueryMeta contains the serviceID for a network meta query
const NetworkQueryMeta string = "weaviate.network.query.meta"

// LocalAdd contains the serviceID for a local add query
const LocalAdd string = "weaviate.local.add"

// LocalAddMeta contains the serviceID for a local add meta query
const LocalAddMeta string = "weaviate.local.add.meta"

// LocalManipulate contains the serviceID for a local manipulate query
const LocalManipulate string = "weaviate.local.manipulate"

// LocalManipulateMeta contains the serviceID for a local manipulate meta query
const LocalManipulateMeta string = "weaviate.local.manipulate.meta"

// LocalTools contains the serviceID for a local tools query
const LocalTools string = "weaviate.local.tools"

// NetworkToolsMap contains the serviceID for a network tools query
const NetworkToolsMap string = "weaviate.network.tools.map"

// TypeGQL contains the serviceID for a local query
const TypeGQL string = "GQL"

// TypeREST contains the serviceID for a local query
const TypeREST string = "REST"

// ReportPostFail contains the value used when a report is unable to be sent to the specified URL
const ReportPostFail string = "[POST failed]"

// ReportCBORFail contains the value used when a report fails to be converted to CBOR
const ReportCBORFail string = "[CBOR conversion failed]"
