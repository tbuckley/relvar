var Relvar = require('./relvar');
var RBTree = require('bintrees').RBTree;
var utils = require('./utils');

// if we have strict ordering guarantees (all nodes 1-step away will get notified before
// any nodes 2 steps away)
// Then a single node can detect when a change comes from ones of it's duplicated dependencies
// and always wait for the highest-up dependency to provide that change
// 
// 
// Relvar.id -- unique ID
// Relvar.insert(rows, cbOrRelvarID)
// Relvar.children
// Relvar.isComputed

function extend(relvar, properties) {
	var spec, prop, r;

	// @TODO ensure no conflict in property names

	// Create new spec
	spec = {};
	for(prop in relvar.spec) {
		spec[prop] = relvar.spec[prop];
	}
	for(prop in properties) {
		spec[prop] = properties[prop].type;
	}
	
	// Define how to handle changes in relvar
	r = new Relvar(spec, relvar.uniqueKey);
	relvar.on("insert", function(rows) {
		var extendedRows = utils.applyExtensionsAll(rows, properties);
		r.insert(extendedRows);
	});
	relvar.on("update", function(keys, rows) {
		var extendedKeys = utils.applyExtensionsAll(keys, properties);
		var extendedRows = utils.applyExtensionsAll(rows, properties);
		r.update(extendedKeys, extendedRows);
	});
	relvar.on("remove", function(rows) {
		// No need to extend since uniqueKey is the same
		r.remove(rows);
	});

	// Initialize relvar
	relvar.index.each(function(row) {
		r.index.insert(utils.applyExtensions(row, properties));
	});
	return r;
}

// @TODO factor out shared code with extend
function project(relvar, properties, optUniqueKey) {
	var spec, prop, r, uniqueKey;

	// Create new spec
	spec = {};
	properties.forEach(function(prop) {
		if(!relvar.spec.hasOwnProperty(prop)) {
			throw new Error("property '"+prop+"' not in Relvar spec");
		}
		spec[prop] = relvar.spec[prop];
	});

	// Choose a uniqueKey
	uniqueKey = optUniqueKey || relvar.uniqueKey;
	
	// Define how to handle changes in relvar
	r = new Relvar(spec, uniqueKey);
	relvar.on("insert", function(rows) {
		var projectedRows = utils.projectAll(rows, properties);
		r.insert(projectedRows);
	});
	relvar.on("update", function(keys, rows) {
		var projectedKeys = utils.projectAll(keys, properties);
		var projectedRows = utils.projectAll(rows, properties);
		r.update(projectedKeys, projectedRows);
	});
	relvar.on("remove", function(rows) {
		// No need to project since it's the same
		r.remove.apply(r, rows);
	});

	// Initialize relvar
	relvar.index.each(function(row) {
		r.index.insert(utils.project(row, properties));
	});
	return r;
}

function union(relvarA, relvarB) {
	var r;

	// @TODO ensure specs/uniqueKeys match

	r = new Relvar(relvarA.spec, relvarA.uniqueKey);

	[relvarA, relvarB].forEach(function(base) {
		base.on("insert", function(rows) {
			r.insert(rows);
		});
		base.on("update", function(keys, rows) {
			r.update(keys, rows);
		});
		base.on("remove", function(rows) {
			r.remove(rows);
		});

		// Initialize relvar
		base.index.each(function(row) {
			r.index.insert(row);
		});
	});

	return r;
}

function difference(relvarA, relvarB) {
	// Question: can we tell two rows are the same by uniqueKey?
	// Answer: no, since they may have different values and
	// we aren't merging them
	
	// Some ops can be made more efficient through 
	// RBTree.lowerBound()/RBTree.upperBound()

	var r;

	r = new Relvar(relvarA.spec, relvarA.uniqueKey);
	relvarA.on("insert", function(rows) {
		// Add any rows that aren't in B
		var filteredRows = [];
		rows.forEach(function(row) {
			if(!relvarB.index.find(row)) {
				filteredRows.push(row);
			}
		});
		r.insert(filteredRows);
	});
	relvarA.on("update", function(keys, rows) {
		var updateKeys = [], updateRows = [], insert = [], remove = [];
		// Update any rows that are in result where key is the same
		// Add any rows where key is in B but row is not
		// Remove any rows where key is not it B but row is
		keys.forEach(function(key, index) {
			var row = rows[index];
			var bHasKey = relvarB.index.find(key);
			var bHasRow = relvarB.index.find(row);
			if(bHasKey && !bHasRow) {
				// Insert
				insert.push(row);
			} else if(!bHasKey && bHasRow) {
				// Remove
				remove.push(key);
			} else if(!bHasKey && !bHasRow) {
				// Update
				updateKeys.push(key);
				updateRows.push(row);
			}
		});
		if(insert.length > 0) {
			r.insert(insert);
		}
		if(remove.length > 0) {
			r.remove(remove);
		}
		if(updateKeys.length > 0) {
			r.update(updateKeys, updateRows);
		}
	});
	relvarA.on("remove", function(rows) {
		// Remove any rows that aren't in B
		var filteredRows = [];
		rows.forEach(function(row) {
			if(!relvarB.index.find(row)) {
				filteredRows.push(row);
			}
		});
		r.remove(filteredRows);
	});

	relvarB.on("insert", function(rows) {
		// Remove any rows that are in A
		var filteredRows = [];
		rows.forEach(function(row) {
			if(relvarA.index.find(row)) {
				filteredRows.push(row);
			}
		});
		r.remove(filteredRows);
	});
	relvarB.on("update", function(keys, rows) {
		// Add any rows in A that are in keys
		// Remove any rows in A that are in rows
		var insert = [], remove = [];
		keys.each(function(key, index) {
			var row = rows[index];
			var aKey = relvarA.index.find(key);
			var aRow = relvarA.index.find(row);
			if(aKey) {
				remove.push(aKey);
			}
			if(aRow) {
				insert.push(aRow);
			}
		});
		if(insert.length > 0) {
			r.insert(insert);
		}
		if(remove.length > 0) {
			r.remove(remove);
		}
	});
	relvarB.on("remove", function(rows) {
		// Add any rows that are in A
		var filteredRows = [];
		rows.forEach(function(row) {
			if(relvarA.index.find(row)) {
				filteredRows.push(row);
			}
		});
		r.add(filteredRows);
	});

	// Initialize relvar
	var diff = utils.rbtreeDifference(relvarA.index, relvarB.index, r.compare.bind(r));
	diff.forEach(function(row) {
		r.index.insert(row);
	});
	return r;
}

function join(relvarLeft, relvarRight, uniqueKey) {
	var r, overlapKey, leftIndex, rightIndex, cmpFunc, joinedSpec, prop;

	overlapKey = utils.getOverlappingProperties(relvarLeft.spec, relvarRight.spec);
	cmpFunc = function(a, b) {
		return utils.compareKey(overlapKey, a.key, b.key);
	};
	leftIndex = new RBTree(cmpFunc);
	rightIndex = new RBTree(cmpFunc);

	joinedSpec = {};
	for(prop in relvarLeft.spec) {
		joinedSpec[prop] = relvarLeft.spec[prop];
	}
	for(prop in relvarRight.spec) {
		joinedSpec[prop] = relvarRight.spec[prop];
	}

	r = new Relvar(joinedSpec, uniqueKey);

	var a = {relvar: relvarLeft, index: leftIndex};
	var b = {relvar: relvarRight, index: rightIndex};

	[[a, b], [b, a]].forEach(function(pair) {
		var relvarA, indexA, relvarB, indexB;
		relvarA = pair[0].relvar;
		indexA = pair[0].index;
		relvarB = pair[1].relvar;
		indexB = pair[1].index;

		relvarA.on("insert", function(rows) {
			// For each row added, add any joins with rows in relvarRight
			var insert;
			insert = [];
			rows.forEach(function(row) {
				utils.addToIndex(overlapKey, indexA, row);
				utils.forEachJoin(overlapKey, indexB, row, function(joiningRow) {
					insert.push(utils.join(row, joiningRow));
				});
			});
			if(insert.length > 0) {
				r.insert(insert);
			}
		});
		relvarA.on("update", function(keys, rows) {
			// If row has same shared properties as before, update
			// If row has different shared properties, insert/remove
			var insert = [], updateKeys = [], updateRows = [], remove = [];
			keys.forEach(function(key, index) {
				var row;
				row = rows[index];
				// If overlapKey properties are the same, it's an update
				if(utils.compareKey(overlapKey, key, row) === 0) {
					utils.forEachJoin(overlapKey, indexB, key, function(joinedRow) {
						updateKeys.push(utils.join(key, joinedRow));
						updateRows.push(utils.join(row, joinedRow));
					});
				} else {
					// Remove key
					utils.removeFromIndex(overlapKey, relvarA.uniqueKey, indexA, key);
					utils.forEachJoin(overlapKey, indexB, key, function(joinedRow) {
						remove.push(utils.join(key, joinedRow));
					});
					// Insert row
					utils.addToIndex(overlapKey, indexA, row);
					utils.forEachJoin(overlapKey, indexB, row, function(joinedRow) {
						insert.push(utils.join(row, joinedRow));
					});
				}
			});
			if(insert.length > 0) {
				r.insert(insert);
			}
			if(updateKeys.length > 0) {
				r.update(updateKeys, updateRows);
			}
			if(remove.length > 0) {
				r.remove(remove);
			}
		});

		relvarA.on("remove", function(rows) {
			// For each row removed, remove any joins with rows in relvarRight
			var remove = [];
			rows.forEach(function(row) {
				utils.removeFromIndex(overlapKey, relvarA.uniqueKey, indexA, row);
				utils.forEachJoin(overlapKey, indexB, row, function(joinedRow) {
					remove.push(utils.join(row, joinedRow));
				});
			});
			if(remove.length > 0) {
				r.remove(remove);
			}
		});
	});

	relvarLeft.index.each(function(row) {
		utils.addToIndex(overlapKey, leftIndex, row);
	});
	relvarRight.index.each(function(row) {
		utils.addToIndex(overlapKey, rightIndex, row);
	});

	relvarLeft.index.each(function(row) {
		utils.forEachJoin(overlapKey, rightIndex, row, function(joiningRow) {
			r.index.insert(utils.join(row, joiningRow));
		});
	});

	return r;
}

exports.extend = extend;
exports.union = union;
exports.difference = difference;
exports.project = project;
exports.join = join;