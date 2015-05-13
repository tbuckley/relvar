exports.compareKey = function(key, a, b) {
	var i, prop;
	for(i = 0; i < key.length; i++) {
		prop = key[i];
		if(a[prop] < b[prop]) {
			return -1;
		}
		if(a[prop] > b[prop]) {
			return 1;
		}
	}
	return 0;
};

exports.shallowCopy = function(obj) {
	var newObj = {};
	for(var key in obj) {
		if(obj.hasOwnProperty(key)) {
			newObj[key] = obj[key];
		}
	}
	return newObj;
};

exports.applyExtensions = function(row, properties) {
	var prop, rowCopy;
	rowCopy = exports.shallowCopy(row);
	for(prop in properties) {
		if(properties[prop].val !== undefined) {
			rowCopy[prop] = properties[prop].val;
		} else if(properties[prop].func) {
			rowCopy[prop] = properties[prop].func(rowCopy);
		}
	}
	return rowCopy;
};
exports.applyExtensionsAll = function(rows, properties) {
	return rows.map(function(row) {
		return exports.applyExtensions(row, properties);
	});
};

exports.project = function(row, properties) {
	var projectedRow = {};
	properties.forEach(function(prop) {
		projectedRow[prop] = row[prop];
	});
	return projectedRow;
};
exports.projectAll = function(rows, properties) {
	return rows.map(function(row) {
		return exports.project(row, properties);
	});
};

exports.getPropertiesForSpec = function(spec) {
	var properties = [];
	for(var prop in spec) {
		if(spec.hasOwnProperty(prop)) {
			properties.push(prop);
		}
	}
	return properties;
};

exports.getOverlappingProperties = function(specA, specB) {
	var properties = [];
	for(var prop in specA) {
		if(specB.hasOwnProperty(prop)) {
			properties.push(prop);
		}
	}
	return properties;
};

exports.validateUniqueKey = function(key, spec) {
	var props;
	if(!key.length) {
		throw new Error("uniqueKey is invalid");
	}

	// Check that key is subset of properties
	props = exports.getPropertiesForSpec(spec);
	if(!exports.arrayIsSubset(key, props)) {
		throw new Error("uniqueKey is not subset of properties");
	}
};

exports.arrayIsSubset = function(a, b) {
	var missing = 0;
	a.forEach(function(val) {
		var index = b.indexOf(val);
		if(index === -1) {
			missing += 1;
		}
	});
	return (missing === 0);
};

exports.rbtreeDifference = function(a, b, cmp) {
	var ait, bit, aitem, bitem, ord, values;
	ait = a.iterator();
	bit = b.iterator();
	values = [];

	aitem = ait.next();
	bitem = bit.next();
	while(aitem || bitem) {
		if(!aitem) {
			return values;
		} else if(!bitem) {
			values.push(aitem);
			aitem = ait.next();
		} else {
			ord = cmp(aitem, bitem);
			if(ord < 0) {
				values.push(aitem);
				aitem = ait.next();
			} else if(ord > 0) {
				bitem = bit.next();
			} else {
				aitem = ait.next();
				bitem = bit.next();
			}
		}
	}
	return values;
};

exports.join = function(a, b) {
	var prop, join;
	join = {};
	for(prop in a) {
		if(a.hasOwnProperty(prop)) {
			join[prop] = a[prop];
		}
	}
	for(prop in b) {
		if(b.hasOwnProperty(prop)) {
			join[prop] = b[prop];
		}
	}
	return join;
};

exports.addToIndex = function(indexKey, index, row) {
	var key, data;
	key = exports.project(row, indexKey);
	data = index.find({key: key});
	if(!data) {
		index.insert({
			key: key,
			rows: [row],
		});
	} else {
		data.rows.push(row);
	}
};
exports.removeFromIndex = function(indexKey, uniqueKey, index, row) {
	var key, data, i;
	key = exports.project(row, indexKey);
	data = index.find({key: key});
	for(i = 0; i < data.rows.length; i++) {
		if(exports.compareKey(uniqueKey, data.rows[i], row) === 0) {
			data.rows.splice(i, 1);
			return;
		}
	}
};
exports.forEachJoin = function(indexKey, index, row, fn) {
	key = exports.project(row, indexKey);
	data = index.find({key: key});
	if(data && data.rows.length > 0) {
		data.rows.forEach(function(joiningRow) {
			fn(joiningRow);
		});
	}
};