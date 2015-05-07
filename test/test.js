var relvar = require("../lib/relvar");
var should = require("should-promised");

// var prev = should.extend('must', Object.prototype);

describe("Relvar", function() {
	describe("#insert()", function() {
		var r, val, val2, invalidVal;

		beforeEach("create table and value", function() {
			r = new relvar.Relvar({
				foo: Number,
				bar: String,
				baz: Boolean,
			});
			val = {
				foo: 42,
				bar: "hello world!",
				baz: true,
			};
			val2 = {
				foo: 6*9,
				bar: "twas brillig",
				baz: false,
			};
			invalidVal = {
				foo: "42",
				bar: "hello world!",
				baz: true,
			};
		});
		it("should trigger an `insert` event for valid values", function(done) {
			this.timeout(100);
			r.on("insert", function(rows) {
				rows.should.be.an.Array;
				rows.should.have.length(1, "Incorrect number of rows");
				rows.should.containDeep([val]);
				done();
			});
			r.insert(val);
		});
		it("should trigger an `insert` event in the nextTick", function(done) {
			this.timeout(100);
			r.insert(val);
			r.on("insert", function(rows) {
				rows.should.be.an.Array;
				rows.should.have.length(1, "Incorrect number of rows");
				rows.should.containDeep([val]);
				done();
			});
		});
		it("should throw an Error for invalid values", function() {
			(function() {
				r.insert(invalidVal);
			}).should.throw(Error);
		});
		it("should return the relvar for valid values", function() {
			r.insert(val).should.equal(r);
		});
		it("should handle multiple values", function(done) {
			this.timeout(100);
			r.on("insert", function(rows) {
				rows.should.be.an.Array;
				rows.should.have.length(2, "Incorrect number of rows");
				rows.should.containDeep([val, val2]);
				done();
			});
			r.insert(val, val2);
		});
	});
});

describe("extend", function() {
	var base, r, vals, valProps, funcProps;
	beforeEach("create test objects", function() {
		base = new relvar.Relvar({
			foo: Number,
			bar: String,
			baz: Boolean,
		});
		vals = [
			{foo: 42, bar: "Hello, world!", baz: true},
			{foo: 25, bar: "A good age", baz: false},
		];
		valProps = {
			qux: {
				type: String,
				val: "Goodbye!",
			},
		};
		funcProps = {
			qux: {
				type: String,
				func: function(row) {
					if(row.foo == 42) {
						return "The meaning of life, the universe, and everything";
					}
					return "Just some number...";
				},
			},
		};
	});
	it("should return a Relvar", function() {
		r = relvar.extend(base, valProps);
		r.should.be.an.instanceof(relvar.Relvar, "Return is not a Relvar");
		r.spec.should.have.property("foo", Number);
		r.spec.should.have.property("qux", String);
	});
	it("should start with any rows of its base", function() {
		base.insert.apply(base, vals);
		r = relvar.extend(base, valProps);
		r.rows.should.have.length(vals.length);
	});
	it("should add a row when its base adds a row", function(done) {
		this.timeout(100);
		r = relvar.extend(base, valProps);
		r.on("insert", function(rows) {
			rows.should.have.length(vals.length);
			rows[0].should.have.property("foo");
			rows[0].should.have.property("qux");
			done();
		});
		base.insert.apply(base, vals);
	});
	it("should handle value properties", function() {
		base.insert.apply(base, vals);
		r = relvar.extend(base, valProps);
		r.rows[0].should.have.property("qux", "Goodbye!");
		r.rows[1].should.have.property("qux", "Goodbye!");
	});
	it("should handle function properties", function() {
		base.insert.apply(base, vals);
		r = relvar.extend(base, funcProps);
		r.rows[0].should.have.property("qux", "The meaning of life, the universe, and everything");
		r.rows[1].should.have.property("qux", "Just some number...");
	});
});

describe("union", function() {
	var baseA, baseB, r, valsA, valsB;
	beforeEach("create test objects", function() {
		baseA = new relvar.Relvar({
			foo: Number,
			bar: String,
			baz: Boolean,
		});
		baseB = new relvar.Relvar({
			foo: Number,
			bar: String,
			baz: Boolean,
		});
		valsA = [
			{foo: 42, bar: "Hello, world!", baz: true},
			{foo: 25, bar: "A good age", baz: false},
		];
		valsB = [
			{foo: 1, bar: "A", baz: true},
			{foo: 2, bar: "B", baz: false},
			{foo: 3, bar: "C", baz: true},
		];
	});
	it("should return a Relvar", function() {
		r = relvar.union(baseA, baseB);
		r.should.be.an.instanceof(relvar.Relvar, "Return is not a Relvar");
		r.spec.should.have.property("foo", Number);
		r.spec.should.have.property("bar", String);
		r.spec.should.have.property("baz", Boolean);
	});
	it("should start with the combined rows of its bases", function() {
		baseA.insert.apply(baseA, valsA);
		baseB.insert.apply(baseB, valsB);
		r = relvar.union(baseA, baseB);
		r.rows.should.have.length(valsA.length + valsB.length);
	});
	it("should add a row when its base adds a row", function(done) {
		this.timeout(100);
		r = relvar.extend(base, valProps);
		r.on("insert", function(rows) {
			rows.should.have.length(vals.length);
			rows[0].should.have.property("foo");
			rows[0].should.have.property("qux");
			done();
		});
		base.insert.apply(base, vals);
	});
	it("should handle value properties", function() {
		base.insert.apply(base, vals);
		r = relvar.extend(base, valProps);
		r.rows[0].should.have.property("qux", "Goodbye!");
		r.rows[1].should.have.property("qux", "Goodbye!");
	});
	it("should handle function properties", function() {
		base.insert.apply(base, vals);
		r = relvar.extend(base, funcProps);
		r.rows[0].should.have.property("qux", "The meaning of life, the universe, and everything");
		r.rows[1].should.have.property("qux", "Just some number...");
	});
});