var Relvar = require('./lib/relvar');
var ops = require('./lib/ops');

module.exports = {
	Relvar: Relvar,
    extend: ops.extend,
    project: ops.project,
    union: ops.union,
    difference: ops.difference,
    join: ops.join,
};
