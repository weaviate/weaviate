# Schema Types

<details>
  <summary><strong>Table of Contents</strong></summary>

  * [Query](#query)
  * [Objects](#objects)
    * [City](#city)
    * [MetaCity](#metacity)
    * [MetaCityMetaObj](#metacitymetaobj)
    * [MetaCityMetaPointingObj](#metacitymetapointingobj)
    * [MetaCityisCapitalObj](#metacityiscapitalobj)
    * [MetaCitylatitudeObj](#metacitylatitudeobj)
    * [MetaCitynameObj](#metacitynameobj)
    * [MetaCitynameTopOccurrencesObj](#metacitynametopoccurrencesobj)
    * [MetaCitypopulationObj](#metacitypopulationobj)
    * [MetaMoveAction](#metamoveaction)
    * [MetaMoveActionMetaObj](#metamoveactionmetaobj)
    * [MetaMoveActionMetaPointingObj](#metamoveactionmetapointingobj)
    * [MetaMoveActioncostObj](#metamoveactioncostobj)
    * [MetaMoveActiondateObj](#metamoveactiondateobj)
    * [MetaMoveActiondateTopOccurrencesObj](#metamoveactiondatetopoccurrencesobj)
    * [MetaMoveActionfromCityObj](#metamoveactionfromcityobj)
    * [MetaMoveActionfromCityPointingObj](#metamoveactionfromcitypointingobj)
    * [MetaMoveActionisMovedObj](#metamoveactionismovedobj)
    * [MetaMoveActionmoveNumberObj](#metamoveactionmovenumberobj)
    * [MetaMoveActionpersonObj](#metamoveactionpersonobj)
    * [MetaMoveActionpersonPointingObj](#metamoveactionpersonpointingobj)
    * [MetaMoveActiontoCityObj](#metamoveactiontocityobj)
    * [MetaMoveActiontoCityTopOccurrencesObj](#metamoveactiontocitytopoccurrencesobj)
    * [MetaPerson](#metaperson)
    * [MetaPersonMetaObj](#metapersonmetaobj)
    * [MetaPersonMetaPointingObj](#metapersonmetapointingobj)
    * [MetaPersonbirthdayObj](#metapersonbirthdayobj)
    * [MetaPersonbirthdayTopOccurrencesObj](#metapersonbirthdaytopoccurrencesobj)
    * [MetaPersonlivesInObj](#metapersonlivesinobj)
    * [MetaPersonlivesInPointingObj](#metapersonlivesinpointingobj)
    * [MoveAction](#moveaction)
    * [Person](#person)
    * [WeaviateLocalConvertedFetchActionsObj](#weaviatelocalconvertedfetchactionsobj)
    * [WeaviateLocalConvertedFetchObj](#weaviatelocalconvertedfetchobj)
    * [WeaviateLocalConvertedFetchThingsObj](#weaviatelocalconvertedfetchthingsobj)
    * [WeaviateLocalHelpersFetchObj](#weaviatelocalhelpersfetchobj)
    * [WeaviateLocalHelpersFetchPinPointObj](#weaviatelocalhelpersfetchpinpointobj)
    * [WeaviateLocalMetaFetchGenericsActionsObj](#weaviatelocalmetafetchgenericsactionsobj)
    * [WeaviateLocalMetaFetchGenericsObj](#weaviatelocalmetafetchgenericsobj)
    * [WeaviateLocalMetaFetchGenericsThingsObj](#weaviatelocalmetafetchgenericsthingsobj)
    * [WeaviateLocalMetaFetchObj](#weaviatelocalmetafetchobj)
    * [WeaviateLocalObj](#weaviatelocalobj)
    * [WeaviateNetworkHelpersFetchObj](#weaviatenetworkhelpersfetchobj)
    * [WeaviateNetworkHelpersFetchOntologyExplorerActionsObj](#weaviatenetworkhelpersfetchontologyexploreractionsobj)
    * [WeaviateNetworkHelpersFetchOntologyExplorerObj](#weaviatenetworkhelpersfetchontologyexplorerobj)
    * [WeaviateNetworkHelpersFetchOntologyExplorerThingsObj](#weaviatenetworkhelpersfetchontologyexplorerthingsobj)
    * [WeaviateNetworkObj](#weaviatenetworkobj)
  * [Enums](#enums)
    * [WeaviateLocalHelpersFetchPinPointSearchTypeEnum](#weaviatelocalhelpersfetchpinpointsearchtypeenum)
    * [WeaviateLocalHelpersFetchPinPointStackEnum](#weaviatelocalhelpersfetchpinpointstackenum)
    * [classEnum](#classenum)
  * [Scalars](#scalars)
    * [Boolean](#boolean)
    * [Float](#float)
    * [ID](#id)
    * [Int](#int)
    * [String](#string)

</details>

## Query (WeaviateObj)
Location of the root query

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Local</strong></td>
<td valign="top"><a href="#weaviatelocalobj">WeaviateLocalObj</a></td>
<td>

Locate on the local Weaviate

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>Network</strong></td>
<td valign="top"><a href="#weaviatenetworkobj">WeaviateNetworkObj</a></td>
<td>

Locate on the Weaviate network

</td>
</tr>
</tbody>
</table>

## Objects

### City

City

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>name</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Official name of the city.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>latitude</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

The city's latitude

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>population</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

Number of inhabitants of the city

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>isCapital</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

True if the city is a capital

</td>
</tr>
</tbody>
</table>

### MetaCity

City

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Meta</strong></td>
<td valign="top"><a href="#metacitymetaobj">MetaCityMetaObj</a></td>
<td>

meta information about class object

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>name</strong></td>
<td valign="top"><a href="#metacitynameobj">MetaCitynameObj</a></td>
<td>

Meta information about the property "name"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>latitude</strong></td>
<td valign="top"><a href="#metacitylatitudeobj">MetaCitylatitudeObj</a></td>
<td>

Meta information about the property "latitude"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>population</strong></td>
<td valign="top"><a href="#metacitypopulationobj">MetaCitypopulationObj</a></td>
<td>

Meta information about the property "population"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>isCapital</strong></td>
<td valign="top"><a href="#metacityiscapitalobj">MetaCityisCapitalObj</a></td>
<td>

Meta information about the property "isCapital"

</td>
</tr>
</tbody>
</table>

### MetaCityMetaObj

meta information about class object

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many class instances are there

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>pointing</strong></td>
<td valign="top"><a href="#metacitymetapointingobj">MetaCityMetaPointingObj</a></td>
<td>

pointing to and from how many other things

</td>
</tr>
</tbody>
</table>

### MetaCityMetaPointingObj

pointing to and from how many other things

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>to</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing to

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>from</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing from

</td>
</tr>
</tbody>
</table>

### MetaCityisCapitalObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>totalTrue</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of boolean value is true

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>percentageTrue</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

percentage of boolean = true

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
</tbody>
</table>

### MetaCitylatitudeObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>lowest</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Lowest value occurrence

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>highest</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Highest value occurrence

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>average</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

average number

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>sum</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

sum of values of found instances

</td>
</tr>
</tbody>
</table>

### MetaCitynameObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>topOccurrences</strong></td>
<td valign="top">[<a href="#metacitynametopoccurrencesobj">MetaCitynameTopOccurrencesObj</a>]</td>
<td>

most frequent property values

</td>
</tr>
</tbody>
</table>

### MetaCitynameTopOccurrencesObj

most frequent property values

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>value</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

property value of the most frequent properties

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>occurs</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

number of occurrance

</td>
</tr>
</tbody>
</table>

### MetaCitypopulationObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>lowest</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Lowest value occurrence

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>highest</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Highest value occurrence

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>average</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

average number

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>sum</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

sum of values of found instances

</td>
</tr>
</tbody>
</table>

### MetaMoveAction

Action of buying a thing

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Meta</strong></td>
<td valign="top"><a href="#metamoveactionmetaobj">MetaMoveActionMetaObj</a></td>
<td>

meta information about class object

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>person</strong></td>
<td valign="top"><a href="#metamoveactionpersonobj">MetaMoveActionpersonObj</a></td>
<td>

Meta information about the property "person"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>toCity</strong></td>
<td valign="top"><a href="#metamoveactiontocityobj">MetaMoveActiontoCityObj</a></td>
<td>

Meta information about the property "toCity"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>fromCity</strong></td>
<td valign="top"><a href="#metamoveactionfromcityobj">MetaMoveActionfromCityObj</a></td>
<td>

Meta information about the property "fromCity"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>isMoved</strong></td>
<td valign="top"><a href="#metamoveactionismovedobj">MetaMoveActionisMovedObj</a></td>
<td>

Meta information about the property "isMoved"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>date</strong></td>
<td valign="top"><a href="#metamoveactiondateobj">MetaMoveActiondateObj</a></td>
<td>

Meta information about the property "date"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>moveNumber</strong></td>
<td valign="top"><a href="#metamoveactionmovenumberobj">MetaMoveActionmoveNumberObj</a></td>
<td>

Meta information about the property "moveNumber"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>cost</strong></td>
<td valign="top"><a href="#metamoveactioncostobj">MetaMoveActioncostObj</a></td>
<td>

Meta information about the property "cost"

</td>
</tr>
</tbody>
</table>

### MetaMoveActionMetaObj

meta information about class object

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many class instances are there

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>pointing</strong></td>
<td valign="top"><a href="#metamoveactionmetapointingobj">MetaMoveActionMetaPointingObj</a></td>
<td>

pointing to and from how many other things

</td>
</tr>
</tbody>
</table>

### MetaMoveActionMetaPointingObj

pointing to and from how many other things

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>to</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing to

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>from</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing from

</td>
</tr>
</tbody>
</table>

### MetaMoveActioncostObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>lowest</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Lowest value occurrence

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>highest</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Highest value occurrence

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>average</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

average number

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>sum</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

sum of values of found instances

</td>
</tr>
</tbody>
</table>

### MetaMoveActiondateObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>topOccurrences</strong></td>
<td valign="top">[<a href="#metamoveactiondatetopoccurrencesobj">MetaMoveActiondateTopOccurrencesObj</a>]</td>
<td>

most frequent property values

</td>
</tr>
</tbody>
</table>

### MetaMoveActiondateTopOccurrencesObj

most frequent property values

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>value</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

property value of the most frequent properties

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>occurs</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

number of occurrance

</td>
</tr>
</tbody>
</table>

### MetaMoveActionfromCityObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>pointing</strong></td>
<td valign="top"><a href="#metamoveactionfromcitypointingobj">MetaMoveActionfromCityPointingObj</a></td>
<td>

pointing to and from how many other things

</td>
</tr>
</tbody>
</table>

### MetaMoveActionfromCityPointingObj

pointing to and from how many other things

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>to</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing to

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>from</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing from

</td>
</tr>
</tbody>
</table>

### MetaMoveActionisMovedObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>totalTrue</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of boolean value is true

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>percentageTrue</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

percentage of boolean = true

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
</tbody>
</table>

### MetaMoveActionmoveNumberObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>lowest</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Lowest value occurrence

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>highest</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

Highest value occurrence

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>average</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

average number

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>sum</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

sum of values of found instances

</td>
</tr>
</tbody>
</table>

### MetaMoveActionpersonObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>pointing</strong></td>
<td valign="top"><a href="#metamoveactionpersonpointingobj">MetaMoveActionpersonPointingObj</a></td>
<td>

pointing to and from how many other things

</td>
</tr>
</tbody>
</table>

### MetaMoveActionpersonPointingObj

pointing to and from how many other things

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>to</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing to

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>from</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing from

</td>
</tr>
</tbody>
</table>

### MetaMoveActiontoCityObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>topOccurrences</strong></td>
<td valign="top">[<a href="#metamoveactiontocitytopoccurrencesobj">MetaMoveActiontoCityTopOccurrencesObj</a>]</td>
<td>

most frequent property values

</td>
</tr>
</tbody>
</table>

### MetaMoveActiontoCityTopOccurrencesObj

most frequent property values

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>value</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

property value of the most frequent properties

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>occurs</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

number of occurrance

</td>
</tr>
</tbody>
</table>

### MetaPerson

Person

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Meta</strong></td>
<td valign="top"><a href="#metapersonmetaobj">MetaPersonMetaObj</a></td>
<td>

meta information about class object

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>livesIn</strong></td>
<td valign="top"><a href="#metapersonlivesinobj">MetaPersonlivesInObj</a></td>
<td>

Meta information about the property "livesIn"

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>birthday</strong></td>
<td valign="top"><a href="#metapersonbirthdayobj">MetaPersonbirthdayObj</a></td>
<td>

Meta information about the property "birthday"

</td>
</tr>
</tbody>
</table>

### MetaPersonMetaObj

meta information about class object

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many class instances are there

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>pointing</strong></td>
<td valign="top"><a href="#metapersonmetapointingobj">MetaPersonMetaPointingObj</a></td>
<td>

pointing to and from how many other things

</td>
</tr>
</tbody>
</table>

### MetaPersonMetaPointingObj

pointing to and from how many other things

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>to</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing to

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>from</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing from

</td>
</tr>
</tbody>
</table>

### MetaPersonbirthdayObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>topOccurrences</strong></td>
<td valign="top">[<a href="#metapersonbirthdaytopoccurrencesobj">MetaPersonbirthdayTopOccurrencesObj</a>]</td>
<td>

most frequent property values

</td>
</tr>
</tbody>
</table>

### MetaPersonbirthdayTopOccurrencesObj

most frequent property values

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>value</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

property value of the most frequent properties

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>occurs</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

number of occurrance

</td>
</tr>
</tbody>
</table>

### MetaPersonlivesInObj

Property meta information

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>type</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

datatype of the property

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>counter</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

total amount of found instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>pointing</strong></td>
<td valign="top"><a href="#metapersonlivesinpointingobj">MetaPersonlivesInPointingObj</a></td>
<td>

pointing to and from how many other things

</td>
</tr>
</tbody>
</table>

### MetaPersonlivesInPointingObj

pointing to and from how many other things

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>to</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing to

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>from</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

how many other classes the class is pointing from

</td>
</tr>
</tbody>
</table>

### MoveAction

Action of buying a thing

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Person</strong></td>
<td valign="top"><a href="#moveactionpersonobj">MoveActionPersonObj</a></td>
<td>

Person who moves

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>toCity</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

The city the person moves to

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>ToCity</strong></td>
<td valign="top"><a href="#moveactiontocityobj">MoveActionToCityObj</a></td>
<td>

The city the person moves to

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>FromCity</strong></td>
<td valign="top"><a href="#moveactionfromcityobj">MoveActionFromCityObj</a></td>
<td>

The city the person moves from

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>isMoved</strong></td>
<td valign="top"><a href="#boolean">Boolean</a></td>
<td>

Whether the person is already moved

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>date</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

The date the person is moving

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>moveNumber</strong></td>
<td valign="top"><a href="#int">Int</a></td>
<td>

The total amount of house moves the person has made

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>cost</strong></td>
<td valign="top"><a href="#float">Float</a></td>
<td>

The total costs of the movement

</td>
</tr>
</tbody>
</table>

### Person

Person

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>LivesIn</strong></td>
<td valign="top"><a href="#personlivesinobj">PersonLivesInObj</a></td>
<td>

The city where the person lives.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>birthday</strong></td>
<td valign="top"><a href="#string">String</a></td>
<td>

Birthday of the person

</td>
</tr>
</tbody>
</table>

### WeaviateLocalConvertedFetchActionsObj

Fetch Actions on the internal Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>MoveAction</strong></td>
<td valign="top">[<a href="#moveaction">MoveAction</a>]</td>
<td>

Action of buying a thing

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
</tbody>
</table>

### WeaviateLocalConvertedFetchObj

Fetch things or actions on the internal Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Things</strong></td>
<td valign="top"><a href="#weaviatelocalconvertedfetchthingsobj">WeaviateLocalConvertedFetchThingsObj</a></td>
<td>

Locate Things on the local Weaviate

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>Actions</strong></td>
<td valign="top"><a href="#weaviatelocalconvertedfetchactionsobj">WeaviateLocalConvertedFetchActionsObj</a></td>
<td>

Locate Actions on the local Weaviate

</td>
</tr>
</tbody>
</table>

### WeaviateLocalConvertedFetchThingsObj

Fetch things on the internal Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>City</strong></td>
<td valign="top">[<a href="#city">City</a>]</td>
<td>

City

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>Person</strong></td>
<td valign="top">[<a href="#person">Person</a>]</td>
<td>

Person

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
</tbody>
</table>

### WeaviateLocalHelpersFetchObj

Fetch things or actions on the internal Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>PinPoint</strong></td>
<td valign="top"><a href="#weaviatelocalhelpersfetchpinpointobj">WeaviateLocalHelpersFetchPinPointObj</a></td>
<td>

Find a set of exact ID's of Things or Actions on the local Weaviate

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_stack</td>
<td valign="top"><a href="#weaviatelocalhelpersfetchpinpointstackenum">WeaviateLocalHelpersFetchPinPointStackEnum</a></td>
<td>

Things or Actions

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_classes</td>
<td valign="top">[<a href="#classenum">classEnum</a>]</td>
<td>

an array of potential classes (they should be in the ontology!)

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_properties</td>
<td valign="top">[<a href="#string">String</a>]</td>
<td>

an array of potential classes

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_needle</td>
<td valign="top"><a href="#string">String</a></td>
<td>

the actual field that will be used in the search.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_searchType</td>
<td valign="top"><a href="#weaviatelocalhelpersfetchpinpointsearchtypeenum">WeaviateLocalHelpersFetchPinPointSearchTypeEnum</a></td>
<td>

the type of search.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

limit of search results

</td>
</tr>
</tbody>
</table>

### WeaviateLocalHelpersFetchPinPointObj

Fetch uuid of Things or Actions on the internal Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>uuid</strong></td>
<td valign="top"><a href="#id">ID</a></td>
<td>

uuid of thing or action pinpointed in fetch query

</td>
</tr>
</tbody>
</table>

### WeaviateLocalMetaFetchGenericsActionsObj

Action to fetch for meta generic fetch

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>MoveAction</strong></td>
<td valign="top"><a href="#metamoveaction">MetaMoveAction</a></td>
<td>

Action of buying a thing

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
</tbody>
</table>

### WeaviateLocalMetaFetchGenericsObj

Object type to fetch

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Things</strong></td>
<td valign="top"><a href="#weaviatelocalmetafetchgenericsthingsobj">WeaviateLocalMetaFetchGenericsThingsObj</a></td>
<td>

Thing to fetch for meta generic fetch

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_maxArraySize</td>
<td valign="top"><a href="#string">String</a></td>
<td>

If there are arrays in the result, limit them to this size

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>Actions</strong></td>
<td valign="top"><a href="#weaviatelocalmetafetchgenericsactionsobj">WeaviateLocalMetaFetchGenericsActionsObj</a></td>
<td>

Action to fetch for meta generic fetch

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_maxArraySize</td>
<td valign="top"><a href="#string">String</a></td>
<td>

If there are arrays in the result, limit them to this size

</td>
</tr>
</tbody>
</table>

### WeaviateLocalMetaFetchGenericsThingsObj

Thing to fetch for meta generic fetch

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>City</strong></td>
<td valign="top"><a href="#metacity">MetaCity</a></td>
<td>

City

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>Person</strong></td>
<td valign="top"><a href="#metaperson">MetaPerson</a></td>
<td>

Person

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
</tbody>
</table>

### WeaviateLocalMetaFetchObj

Fetch things or actions on the internal Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Generics</strong></td>
<td valign="top"><a href="#weaviatelocalmetafetchgenericsobj">WeaviateLocalMetaFetchGenericsObj</a></td>
<td>

Fetch generic meta information based on the type

</td>
</tr>
</tbody>
</table>

### WeaviateLocalObj

Type of fetch on the internal Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>ConvertedFetch</strong></td>
<td valign="top"><a href="#weaviatelocalconvertedfetchobj">WeaviateLocalConvertedFetchObj</a></td>
<td>

Do a converted fetch to search Things or Actions on the local weaviate

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_filter</td>
<td valign="top"><a href="#weaviatelocalconvertedfetchfilterinpobj">WeaviateLocalConvertedFetchFilterInpObj</a></td>
<td>

Filter options for the converted fetch search, to convert the data to the filter input

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>HelpersFetch</strong></td>
<td valign="top"><a href="#weaviatelocalhelpersfetchobj">WeaviateLocalHelpersFetchObj</a></td>
<td>

Do a helpers fetch to search Things or Actions on the local weaviate

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>MetaFetch</strong></td>
<td valign="top"><a href="#weaviatelocalmetafetchobj">WeaviateLocalMetaFetchObj</a></td>
<td>

Fetch meta infromation about Things or Actions on the local weaviate

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_filter</td>
<td valign="top"><a href="#weaviatelocalmetafetchfilterinpobj">WeaviateLocalMetaFetchFilterInpObj</a></td>
<td>

Filter options for the meta fetch search, to convert the data to the filter input

</td>
</tr>
</tbody>
</table>

### WeaviateNetworkHelpersFetchObj

search for things or actions on the network Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>OntologyExplorer</strong></td>
<td valign="top"><a href="#weaviatenetworkhelpersfetchontologyexplorerobj">WeaviateNetworkHelpersFetchOntologyExplorerObj</a></td>
<td>

search for things or actions on the network Weaviate

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_distance</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

maximum distance to other class instances

</td>
</tr>
</tbody>
</table>

### WeaviateNetworkHelpersFetchOntologyExplorerActionsObj

Action to fetch for network fetch

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>MoveAction</strong></td>
<td valign="top">[<a href="#moveaction">MoveAction</a>]</td>
<td>

Action of buying a thing

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
</tbody>
</table>

### WeaviateNetworkHelpersFetchOntologyExplorerObj

search for things or actions on the network Weaviate

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>Things</strong></td>
<td valign="top"><a href="#weaviatenetworkhelpersfetchontologyexplorerthingsobj">WeaviateNetworkHelpersFetchOntologyExplorerThingsObj</a></td>
<td>

Thing to fetch in network

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_distance</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

maximum distance to other instances

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>Actions</strong></td>
<td valign="top"><a href="#weaviatenetworkhelpersfetchontologyexploreractionsobj">WeaviateNetworkHelpersFetchOntologyExplorerActionsObj</a></td>
<td>

Action to fetch in network

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_distance</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

maximum distance to other instances

</td>
</tr>
</tbody>
</table>

### WeaviateNetworkHelpersFetchOntologyExplorerThingsObj

Thing to fetch for network fetch

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>City</strong></td>
<td valign="top">[<a href="#city">City</a>]</td>
<td>

City

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>Person</strong></td>
<td valign="top">[<a href="#person">Person</a>]</td>
<td>

Person

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_certainty</td>
<td valign="top"><a href="#float">Float</a></td>
<td>

How certain about these values?

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_limit</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the max returned values.

</td>
</tr>
<tr>
<td colspan="2" align="right" valign="top">_skip</td>
<td valign="top"><a href="#int">Int</a></td>
<td>

define the amount of values to skip.

</td>
</tr>
</tbody>
</table>

### WeaviateNetworkObj

Type of fetch on the Weaviate network

<table>
<thead>
<tr>
<th align="left">Field</th>
<th align="right">Argument</th>
<th align="left">Type</th>
<th align="left">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="2" valign="top"><strong>FuzzyFetch</strong></td>
<td valign="top">[<a href="#string">String</a>]</td>
<td>

Do a fuzzy search fetch to search Things or Actions on the network weaviate

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>HelpersFetch</strong></td>
<td valign="top"><a href="#weaviatenetworkhelpersfetchobj">WeaviateNetworkHelpersFetchObj</a></td>
<td>

Do a fetch with help to search Things or Actions on the network weaviate

</td>
</tr>
<tr>
<td colspan="2" valign="top"><strong>MetaFetch</strong></td>
<td valign="top">[<a href="#string">String</a>]</td>
<td>

To fetch meta information Things or Actions on the network weaviate

</td>
</tr>
</tbody>
</table>

## Enums

### WeaviateLocalHelpersFetchPinPointSearchTypeEnum

<table>
<thead>
<th align="left">Value</th>
<th align="left">Description</th>
</thead>
<tbody>
<tr>
<td valign="top"><strong>standard</strong></td>
<td></td>
</tr>
</tbody>
</table>

### WeaviateLocalHelpersFetchPinPointStackEnum

<table>
<thead>
<th align="left">Value</th>
<th align="left">Description</th>
</thead>
<tbody>
<tr>
<td valign="top"><strong>Things</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>Actions</strong></td>
<td></td>
</tr>
</tbody>
</table>

### classEnum

enum type which denote the classes

<table>
<thead>
<th align="left">Value</th>
<th align="left">Description</th>
</thead>
<tbody>
<tr>
<td valign="top"><strong>City</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>Person</strong></td>
<td></td>
</tr>
<tr>
<td valign="top"><strong>MoveAction</strong></td>
<td></td>
</tr>
</tbody>
</table>

## Scalars

### Boolean

The `Boolean` scalar type represents `true` or `false`.

### Float

The `Float` scalar type represents signed double-precision fractional values as specified by [IEEE 754](http://en.wikipedia.org/wiki/IEEE_floating_point). 

### ID

The `ID` scalar type represents a unique identifier, often used to refetch an object or as key for a cache. The ID type appears in a JSON response as a String; however, it is not intended to be human-readable. When expected as an input type, any string (such as `"4"`) or integer (such as `4`) input value will be accepted as an ID.

### Int

The `Int` scalar type represents non-fractional signed whole numeric values. Int can represent values between -(2^31) and 2^31 - 1. 

### String

The `String` scalar type represents textual data, represented as UTF-8 character sequences. The String type is most often used by GraphQL to represent free-form human-readable text.

