const _ = require('lodash');
// file system for reading files
const fs = require('fs');
const data = JSON.parse(fs.readFileSync('./demo_resolver/demo_data.json', 'utf8'));

var solve_groupBy = function(filter, list) {
	var groups = {}
	var groupBy = filter.group;
	
	// group the items in the list on the group filter
	for(var i=0; i<list.length; i++) {
		if (list[i][groupBy] in groups) {
			groups[list[i][groupBy]].push(list[i])
		}
		else {
			groups[list[i][groupBy]] = [list[i]]
		}
	}

	var returndict = {};
	for(var group in groups) {
		if (group == 'false') { group = false};
		returndict[group] = {[groupBy]: group};
	}

	// apply the aggregation filters to the grouped data
	if (filter.mean) {
		for(var group in groups) {
			var total = 0; count = 0;
			for(var j=0; j<groups[group].length; j++) {
				total += parseFloat(groups[group][j][filter.mean]);
				count += 1;
			};
			var mean = total/count;
			returndict[group][filter.mean] = mean
		}
	}
	if (filter.maximum) {
		for(var group in groups) {
			var max = -9999999.;
			for(var j=0; j<groups[group].length; j++) {
				if (parseFloat(groups[group][j][filter.maximum]) > max) {
					max = groups[group][j][filter.maximum];
				};
			};
			returndict[group][filter.maximum] = max
		}
	}
	if (filter.minimum) {
		for(var group in groups) {
			var min = 99999999;
			for(var j=0; j<groups[group].length; j++) {
				if (parseFloat(groups[group][j][filter.minimum]) < min) {
					min = groups[group][j][filter.minimum];
				};
			};
			returndict[group][filter.minimum] = min
		}
	}
	if (filter.sum) {
		for(var group in groups) {
			var total = 0;
			for(var j=0; j<groups[group].length; j++) {
				total += parseFloat(groups[group][j][filter.sum])
			};
			returndict[group][filter.sum] = sum
		}
	}
	if (filter.count) {
		for(var group in groups) {
			var count = 0;
			count = groups[group].length
			returndict[group][filter.count] = count
		}
	}


	var returnlist = [];
	for(var i in returndict) {
		returnlist.push(returndict[i]);
	}
	
	return returnlist
}


var solveAggregateRootClass = function(all_data, className, args, location) {
	var list = []
	for(var i=0; i < all_data.length; i++){
		if(all_data[i].class == className){
			list.push(all_data[i])
		}
	}


	if (args.limit) {
		list = list.splice(0, args.limit)
	}
	all_data = list
	
	nodes_in_class = []
	for (var i in all_data) { // loop through single things or actions
		if (all_data[i].class == className) {
			nodes_in_class.push(all_data[i])
		}
	}

	metadata = []
	metadata["class"] = className

	return metadata
}


var solveMetaRootClass = function(nodes_in_class, className, args) {	
	metadata = []
	metadata["class"] = className
	metadata["meta"] = {"count": nodes_in_class.length}
	
	
	for (var key in nodes_in_class[0]) {
		if (key == "class" || key == "uuid") {
			continue
		}
		metadata[key] = {}
		metadata[key]["count"] = 0
		var type = typeof(nodes_in_class[0][key])
		if (type == "object") {
			newkey = key[0].toUpperCase() + key.substring(1)
			metadata[newkey] = {}
			metadata[newkey]["count"] = 0
			metadata[newkey]["type"] = "cref"
			metadata[newkey]["pointingTo"] = [nodes_in_class[0][key]["class"]]
		}
		else if (type == "boolean") {
			metadata[key]["type"] = "boolean"
			metadata[key]["totalTrue"] = 0
			metadata[key]["totalFalse"] = 0
			metadata[key]["percentageTrue"] = 0
			metadata[key]["percentageFalse"] = 0
		}
		else if (type == "number") {
			if (!isNaN(nodes_in_class[0][key])) {
				metadata[key]["type"] = "number"
				metadata[key]["minimum"] = 999999999999
				metadata[key]["maximum"] = -999999999999
				metadata[key]["mean"] = 0
				metadata[key]["sum"] = 0
			}
			else {
				metadata[key]["type"] = "string"
				metadata[key]["topOccurrences"] = []
			}
		}
		else if (type == "string") {
			metadata[key]["type"] = "string"
			metadata[key]["topOccurrences"] = []
		}
	}

	for (var node in nodes_in_class) {
		for (var key in nodes_in_class[node]) {
			if (key == "class" || key == "uuid") {
				continue
			}			
			if (typeof(nodes_in_class[node][key]) == "object") {
				key = key[0].toUpperCase() + key.substring(1)
			}
			metadata[key]["count"] += 1;
			var type = typeof(nodes_in_class[node][key])
			if (type == "boolean") {
				metadata[key]["type"] = "boolean"
				if (nodes_in_class[node][key] == true) {
					metadata[key]["totalTrue"] += 1;
				}
				else {
					metadata[key]["totalFalse"] += 1;
				}
				metadata[key]["percentageTrue"] = (metadata[key]["totalTrue"] / metadata[key]["count"] * 100);
				metadata[key]["percentageFalse"] = (metadata[key]["totalFalse"] / metadata[key]["count"] * 100);
			}
			else if (type == "number") {
				if (!isNaN(nodes_in_class[node][key])) {
					metadata[key]["type"] = "number"
					value = parseFloat(nodes_in_class[node][key])
					if (value < metadata[key]["minimum"]) {
						metadata[key]["minimum"] = value
					}
					if (value > metadata[key]["maximum"]) {
						metadata[key]["maximum"] = value
					}
					metadata[key]["sum"] += value
					metadata[key]["mean"] = (metadata[key]["sum"] / metadata[key]["count"])
				}
			}
			else if (type == "string") {
				metadata[key]["type"] = "string"
				metadata[key]["topOccurrences"].push({"value": nodes_in_class[node][key], "occurs": 1}) // currently doesn't count occurrences
			}
		}
	}

	return metadata
}


var resolve_IE = function (object_list, operator, path, value, location="Local") {
	// loop over filter EQ list
	var return_list = []

	// loop over things/actions in list
	for(var j=0; j < object_list.length; j++) {
		var object = object_list[j]

		for (var p=0; p < path.length; p++) { // loop over rest of items in path			
			if (object.class === path[p]) {
				continue
			}
			else {
				for (var key in object) {
					if (key == path[p] || (key.toLowerCase() == path[p].toLowerCase())) {
						if (p == (path.length - 1)) { // if last item in path list 
							if (operator == "GreaterThan") {
								if (parseFloat(object[path[p]]) > value) {
									//return_list.push(object_list[j])
									return_list = _.union(return_list, [object_list[j]])
								}
							} else if (operator == "LessThan") {
								if (parseFloat(object[path[p]]) < value) {
									//return_list.push(object_list[j])
									return_list = _.union(return_list, [object_list[j]])
								}
							} else if (operator == "GreaterThanEqual") {
								if (parseFloat(object[path[p]]) >= value) {
									//return_list.push(object_list[j])
									return_list = _.union(return_list, [object_list[j]])
								}
							} else if (operator == "LessThanEqual") {
								if (parseFloat(object[path[p]]) <= value) {
									//return_list.push(object_list[j])
									return_list = _.union(return_list, [object_list[j]])
								}
							}
						} else {
							if (path[p][0] !== path[p][0].toUpperCase()) {
								object = object[path[p]]
							}
							else { // object is undefined because capital differences
								prop = path[p][0].toLowerCase() + path[p].substring(1)
								object = object[prop]
							}
						}
						continue
					}
				}
			}
		}
	}
	return return_list
}


var resolve_NEQ = function (object_list, path, value, location="Local") {
	// loop over filter NEQ list
	var return_list = []

	var short_list = Object.assign([], object_list)

	// loop over things/actions in list
	for(var j=0; j < object_list.length; j++) {
		var object = object_list[j]

		for (var p=0; p < path.length; p++) { // loop over rest of items in path
			if (object.class === path[p]) {
				continue
			}
			else { // path item is property (or: string starts with small letter)
				for (var key in object) {
					if (key == path[p] || (key.toLowerCase() == path[p].toLowerCase())) {
						if (p == (path.length - 1)) { // if last item in path list 
							if (value == object[path[p]] || value == String(object[path[p]]).toLowerCase()) { // if property value is same as path prop object value
								var index = short_list.indexOf(object_list[j])
								short_list.splice(index, 1)
							}
						} else {
							if (path[p][0] !== path[p][0].toUpperCase()) {
								object = object[path[p]]
							}
							else { // object is undefined because capital differences
								prop = path[p][0].toLowerCase() + path[p].substring(1)
								object = object[prop]
							}
						}
						continue
					}
				}
			}
		}
	}
	return_list = _.union(return_list, short_list)

	return return_list
}


var resolve_EQ = function (object_list, path, value, location="Local") {
	// loop over filter EQ list
	var new_list = []

	// loop over things/actions in list
	for(var j=0; j < object_list.length; j++) {
		var object = object_list[j]

		for (var p=0; p < path.length; p++) { // loop over rest of items in path
			if (object.class === path[p]) {
				continue
			}
			else { // path item is property (or: string starts with small letter)
				for (var key in object) {
					if (key == path[p] || (key.toLowerCase() == path[p].toLowerCase())) {
						if (p == (path.length - 1)) { // if last item in path list 
							if (value == object[path[p]] || value == String(object[path[p]]).toLowerCase()) { // if property value is same as path prop object value
								new_list = _.union(new_list, [object_list[j]])
							}
						} else {
							if (path[p][0] !== path[p][0].toUpperCase()) {
								object = object[path[p]]
							}
							else { // object is undefined because capital differences
								prop = path[p][0].toLowerCase() + path[p].substring(1)
								object = object[prop]
							}
						}
						continue
					}
				}
			}
		}
	}


	return new_list
}


const solve_path = function (list, operator, path, value, location="Local") {
	

	if (["GreaterThan", "GreaterThanEqual", "LessThan", "LessThanEqual"].includes(operator)) {
		// IE
		return resolve_IE(list, operator, path, value, location)
	}
	else if (operator == "Equal") {
		// EQ
		return resolve_EQ(list, path, value, location)
	}
	else if (operator == "NotEqual") {
		// NEQ
		return resolve_NEQ(list, path, value, location)
	}
}


const solve_operands = function (list, operator, operands, location="Local") {
	return_data = []
	for (var i in operands) {
		operand = operands[i]

		if (operand.operator == 'And' || operand.operator == 'Or') {
			result = solve_operands(list, operand.operator, operand.operands, location)
		}
		else {
			if (operand.valueString) {
				result = solve_path(list, operand.operator, operand.path, operand.valueString, location)
			}
			else if (operand.valueDate) {
				result = solve_path(list, operand.operator, operand.path, operand.valueDate, location)
			}
			else if (operand.valueInt) {
				result = solve_path(list, operand.operator, operand.path, operand.valueInt, location)
			}
			else if (operand.valueBoolean) {
				result = solve_path(list, operand.operator, operand.path, operand.valueBoolean, location)
			}
			else if (operand.valueFloat) {
				result = solve_path(list, operand.operator, operand.path, operand.valueFloat, location)
			}
			else if (operand.valueText) {
				return 'error'
			}
		}
		if (operator == 'And') {
			if (i == 0) {
				return_data = result
			}
			else {
				return_data = _.intersection(result, return_data)
			}
		}
		else if (operator == 'Or') {
			return_data = _.union(result, return_data)

		}
	}
	return return_data
}


const resolve_where = function (list, filter) {
	if (filter.operator == 'And' || filter.operator == 'Or') {
		return solve_operands(list, filter.operator, filter.operands)
	}
	else {
		if (filter.valueString) {
			return solve_path(list, filter.operator, filter.path, filter.valueString)
		}
		else if (filter.valueDate) {
			return solve_path(list, filter.operator, filter.path, filter.valueDate)
		}
		else if (filter.valueInt) {
			return solve_path(list, filter.operator, filter.path, filter.valueInt)
		}
		else if (filter.valueBoolean) {
			return solve_path(list, filter.operator, filter.path, filter.valueBoolean)
		}
		else if (filter.valueFloat) {
			return solve_path(list, filter.operator, filter.path, filter.valueFloat)
		}
		else if (filter.valueText) {
			return 'error'
		}
	}

}


module.exports = {
	resolveGet: function() {
		return data.Local
    },
    rootClassResolver: function(return_data, className, args) {
	    var list = [] // all instances in demo data from this class
	    for(var i=0; i < return_data.length; i++){
	        if(return_data[i].class == className){
			    list.push(return_data[i])
		    }
		}

		if (args.where) {
			list = resolve_where(list, args.where)
		}


	    if (args.limit) {
		    list = list.splice(0, args.limit)
		}
		// if (args.groupBy) {[
		// 	list = solve_groupBy(args.groupBy, list)
		// ]}
		return list
    },
    metaRootClassResolver: function(return_data, className, args) {
		var list = [] // all instances in demo data from this class
	    for(var i=0; i < return_data.length; i++){
	        if(return_data[i].class == className){
			    list.push(return_data[i])
		    }
		}
		if (args.where) {
			list = resolve_where(list, args.where)
		}

	    if (args.limit) {
		    list = list.splice(0, args.limit)
		}
		return solveMetaRootClass(list, className, args)
	},
	aggregateRootClassResolver: function(all_data, className, args, location) {
		return solveAggregateRootClass(all_data, className, args, location)
	},
	resolveNetworkGet: function(filter) {
		all_data = _.clone(data);
		if (filter) {
			path = filter.path
			if (filter.operator == 'And' || filter.operator == 'Or') {
				return solve_operands(filter.operator, filter.operands, location=filter.operands[0].path[0])
			}
			else {
				if (filter.valueString) {
					return solve_path(filter.operator, filter.path, filter.valueString, location=path[0])
				}
				else if (filter.valueDate) {
					return solve_path(filter.operator, filter.path, filter.valueDate, location=path[0])
				}
				else if (filter.valueInt) {
					return solve_path(filter.operator, filter.path, filter.valueInt, location=path[0])
				}
				else if (filter.valueBoolean) {
					return solve_path(filter.operator, filter.path, filter.valueBoolean, location=path[0])
				}
				else if (filter.valueFloat) {
					return solve_path(filter.operator, filter.path, filter.valueFloat, location=path[0])
				}
				else if (filter.valueText) {
					return 'error'
				}
			}
		}
		else {
			return data
		}
	},
	resolveNetworkFetch: function(args){
		var argsClassName = args.where.class.name
		var argsKeywordsValue = args.where.class.keywords[0].value
		var returnlist = []

		for (var i in networkNodes) {
			var node = networkNodes[i];
			if (node.className == argsClassName) {
				node["certainty"] = 0.98
				returnlist.push(node)
			} else if (node.className == argsKeywordsValue) {
				node["certainty"] = 0.73
				returnlist.push(node)
			}
		}
		return returnlist
	},
	resolveNetworkFetchFuzzy: function(args){
		var value = args.value
		var certainty = args.certainty
		var returnlist = []

		for (var i in networkNodes) {
			var node = networkNodes[i];
			for(var property in networkNodes[i].properties){
				if (networkNodes[i].properties[property].valueString == value){
					node["certainty"] = 0.98
					returnlist.push(node)
				}
			}
		}
		return returnlist
	},
	resolveNetworkIntrospect: function(args){
		var argsClassName = args.where[0].class.name
		var argsKeywordsValue = args.where[0].class.keywords[0].value
		var returnlist = []

		for (var i in networkNodes) {
			var node = networkNodes[i];
			if (node.className == argsClassName) {
				node["certainty"] = 0.98
				for (var j in node.properties) {
					node.properties[j]["propertyName"] = node.properties[j].name
					node.properties[j]["certainty"] = 0.96
				}
				var found = false;
				for (var k in returnlist) {
					if (returnlist[k].className == node.className) {
						found = true;
					}
				} 
				if (!found) {
					returnlist.push(node)
				}
				if (returnlist.length == 0) {
					returnlist.push(node)
				}
			} else if (node.className == argsKeywordsValue) {
				node["certainty"] = 0.73
				for (var j in node.properties) {
					node.properties[j]["propertyName"] = node.properties[j].name
					node.properties[j]["certainty"] = 0.96
				}
				var found = false;
				for (var k in returnlist) {
					if (returnlist[k].className == node.className) {
						found = true;
					}
				} 
				if (!found) {
					returnlist.push(node)
				}
				if (returnlist.length == 0) {
					returnlist.push(node)
				}
			}
		}
		return returnlist
	},
	resolveNetworkIntrospectBeacon: function(args) {
		var id = args.id;
		for (var i in networkNodes) {
			node = networkNodes[i];
			if (node.beacon == id) {
				//node["certainty"] = 0.87
				for (var j in node.properties) {
					node.properties[j]["propertyName"] = node.properties[j].name
					//node.properties[j]["certainty"] = 0.96
				}
				return node
			}
		}
	}
}

const networkNodes = [{
	"beacon": "weaviate://weaviateB/8569c0aa-3e8a-4de4-86a7-89d010152ad6",
	"weaviate": "weaviateB",
	"className": "City",
	"properties": [{
		"name": "name",
		"valueString": "Amsterdam"
	}, {
		"name": "population",
		"valueInt": 360000
	}]
}, {
	"beacon": "weaviate://weaviateB/8569c0aa-3e8a-4de4-86a7-89d010152ad7",
	"weaviate": "weaviateB",
	"className": "City",
	"properties": [{
		"name": "name",
		"valueString": "Rotterdam"
	}, {
		"name": "population",
		"valueInt": 250000
	}]
}, {
	"beacon": "weaviate://weaviateB/8569c0aa-3e8a-4de4-86a7-89d010152ad8",
	"weaviate": "weaviateB",
	"className": "City",
	"properties": [{
		"name": "name",
		"valueString": "Utrecht"
	}, {
		"name": "population",
		"valueInt": 200000
	}]
}, {
	"beacon": "weaviate://weaviateC/8569c0aa-3e8a-4de4-86a7-89d010152ad9",
	"weaviate": "weaviateC",
	"className": "Place",
	"properties": [{
		"name": "label",
		"valueString": "Amsterdam"
	}, {
		"name": "inhabitants",
		"valueInt": 360000
	}]
}, {
	"beacon": "weaviate://weaviateC/8569c0aa-3e8a-4de4-86a7-89d010152ad0",
	"weaviate": "weaviateC",
	"className": "Place",
	"properties": [{
		"name": "label",
		"valueString": "Rotterdam"
	}, {
		"name": "inhabitants",
		"valueInt": 250000
	}]
}, {
	"beacon": "weaviate://weaviateC/8569c0aa-3e8a-4de4-86a7-89d010152ad1",
	"weaviate": "weaviateC",
	"className": "Place",
	"properties": [{
		"name": "label",
		"valueString": "Utrecht"
	}, {
		"name": "inhabitants",
		"valueInt": 200000
	}]
}]
