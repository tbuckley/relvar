

function hasSameUniqueKey_(key, a, b) {
	for(var i = 0; i < key.length; i++) {
		var prop = key[i];
		if(a[prop] !== b[prop]) {
			return false;
		}
	}
	return true;
}

function shallowCopy_(obj) {
	var newObj = {};
	for(var key in obj) {
		if(obj.hasOwnProperty(key)) {
			newObj[key] = obj[key];
		}
	}
	return newObj;
}

function applyExtensions_(rows, properties) {
	return rows.map(function(row) {
		var prop, rowCopy;
		rowCopy = shallowCopy_(row);
		for(prop in properties) {
			if(properties[prop].val) {
				rowCopy[prop] = properties[prop].val;
			} else if(properties[prop].func) {
				rowCopy[prop] = properties[prop].func(rowCopy);
			}
		}
		return rowCopy;
	});
}

function project_(rows, properties) {
	return rows.map(function(row) {
		var projectedRow = {};
		properties.forEach(function(prop) {
			projectedRow[prop] = row[prop];
		});
		return projectedRow;
	});
}

exports.getPropertiesForSpec = function(spec) {
	var properties = [];
	for(var prop in spec) {
		if(spec.hasOwnProperty(prop)) {
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