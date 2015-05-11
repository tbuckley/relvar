var relvar = require("../");
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
			}, ["foo"]);
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
			r.insert([val]);
		});
		it("should trigger an `insert` event asynchronously", function(done) {
			this.timeout(100);
			r.insert([val]);
			r.on("insert", function(rows) {
				rows.should.be.an.Array;
				rows.should.have.length(1, "Incorrect number of rows");
				rows.should.containDeep([val]);
				done();
			});
		});
		it("should call a callback when done", function(done) {
			this.timeout(100);
			r.insert([val], function() {
				done();
			});
		});
		it("should update the values in the relvar when done", function(done) {
			this.timeout(100);
			r.insert([val], function() {
				rows = r.array();
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
			r.insert([val]).should.equal(r);
		});
		it("should handle multiple values", function(done) {
			this.timeout(100);
			r.on("insert", function(rows) {
				rows.should.be.an.Array;
				rows.should.have.length(2, "Incorrect number of rows");
				rows.should.containDeep([val, val2]);
				done();
			});
			r.insert([val, val2]);
		});
	});
});

describe("Relvar", function() {
	describe("#remove()", function() {
		var r, vals, val, val2, invalidVal;

		beforeEach("create table and value", function(done) {
			r = new relvar.Relvar({
				foo: Number,
				bar: String,
				baz: Boolean,
			}, ["foo"]);
			vals = [
				{foo: 1, bar: "A", baz: true},
				{foo: 2, bar: "B", baz: false},
				{foo: 3, bar: "C", baz: true},
			];
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
			r.insert(vals, function() {
				r.insert([val, val2], done);
			});
		});
		it("should trigger an `remove` event for existing rows", function(done) {
			this.timeout(100);
			r.on("remove", function(rows) {
				rows.should.be.an.Array;
				rows.should.have.length(1, "Incorrect number of rows");
				rows.should.containDeep([val]);
				done();
			});
			r.remove([val]);
		});
		it("should trigger an `insert` event asynchronously", function(done) {
			this.timeout(100);
			r.remove([val]);
			r.on("remove", function(rows) {
				rows.should.be.an.Array;
				rows.should.have.length(1, "Incorrect number of rows");
				rows.should.containDeep([val]);
				done();
			});
		});
		it("should call a callback when done", function(done) {
			this.timeout(100);
			r.remove([val], function() {
				done();
			});
		});
		it("should update the values in the relvar when done", function(done) {
			this.timeout(100);
			r.remove([val], function() {
				rows = r.array();
				rows.should.have.length(vals.length+1, "Incorrect number of rows");
				rows.should.containDeep(vals);
				rows.should.containDeep([val2]);
				done();
			});
		});
		it("should throw an Error for invalid values", function() {
			(function() {
				r.insert(invalidVal);
			}).should.throw(Error);
		});
		it("should return the relvar for valid values", function() {
			r.remove([val]).should.equal(r);
		});
		it("should handle multiple values", function(done) {
			this.timeout(100);
			r.on("remove", function(rows) {
				rows.should.be.an.Array;
				rows.should.have.length(2, "Incorrect number of rows");
				rows.should.containDeep([val, val2]);
				done();
			});
			r.remove([val, val2]);
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
		}, ["foo"]);
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
	it("should return a Relvar with the extended spec", function() {
		r = relvar.extend(base, valProps);
		r.should.be.an.instanceof(relvar.Relvar, "Return is not a Relvar");
		r.spec.should.have.property("foo", Number);
		r.spec.should.have.property("bar", String);
		r.spec.should.have.property("baz", Boolean);
		r.spec.should.have.property("qux", String);
	});
	it("should start with any rows of its base", function(done) {
		this.timeout(100);
		base.insert.apply(base, vals);
		process.nextTick(function() {
			r = relvar.extend(base, valProps);
			r.rows.should.have.length(vals.length);
			done();
		});
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
	it("should handle value properties", function(done) {
		this.timeout(100);
		base.insert.apply(base, vals);
		r = relvar.extend(base, valProps);
		r.on("insert", function(rows) {
			r.rows.should.have.length(vals.length);
			r.rows[0].should.have.property("qux", "Goodbye!");
			r.rows[1].should.have.property("qux", "Goodbye!");
			done();
		});
	});
	it("should handle function properties", function() {
		base.insert.apply(base, vals);
		r = relvar.extend(base, funcProps);
		r.on("insert", function(rows) {
			r.rows.should.have.length(vals.length);
			r.rows[0].should.have.property("qux", "The meaning of life, the universe, and everything");
			r.rows[1].should.have.property("qux", "Just some number...");
		});
	});
});

describe("project", function() {
	var base, r, vals, props, uniqueKey;
	beforeEach("create test objects", function() {
		base = new relvar.Relvar({
			foo: Number,
			bar: String,
			baz: Boolean,
		}, ["foo"]);
		uniqueKey = ["foo"];
		vals = [
			{foo: 42, bar: "Hello, world!", baz: true},
			{foo: 25, bar: "A good age", baz: false},
		];
		props = ["foo", "baz"];
	});
	it("should return a Relvar with the projected spec", function() {
		r = relvar.project(base, props, uniqueKey);
		r.should.be.an.instanceof(relvar.Relvar, "Return is not a Relvar");
		r.spec.should.have.property("foo", Number);
		r.spec.should.have.property("baz", Boolean);
		r.spec.should.not.have.property("bar");
	});
	it("should start with any rows of its base", function(done) {
		this.timeout(100);
		base.insert.apply(base, vals);
		process.nextTick(function() {
			r = relvar.project(base, props, uniqueKey);
			r.rows.should.have.length(vals.length);
			r.rows.forEach(function(row) {
				row.should.have.property("foo");
				row.should.not.have.property("bar");
				row.should.have.property("baz");
			});
			done();
		});
	});
	it("should add a row when its base adds a row", function(done) {
		this.timeout(100);
		r = relvar.project(base, props, uniqueKey);
		r.on("insert", function(rows) {
			rows.should.have.length(vals.length);
			rows.forEach(function(row) {
				row.should.have.property("foo");
				row.should.not.have.property("bar");
				row.should.have.property("baz");
			});
			done();
		});
		base.insert.apply(base, vals);
	});
});

describe("union", function() {
	var baseA, baseB, r, valsA, valsB;
	beforeEach("create test objects", function() {
		baseA = new relvar.Relvar({
			foo: Number,
			bar: String,
			baz: Boolean,
		}, ["foo"]);
		baseB = new relvar.Relvar({
			foo: Number,
			bar: String,
			baz: Boolean,
		}, ["foo"]);
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
	it("should return a Relvar with the same spec", function() {
		r = relvar.union(baseA, baseB);
		r.should.be.an.instanceof(relvar.Relvar, "Return is not a Relvar");
		r.spec.should.have.property("foo", Number);
		r.spec.should.have.property("bar", String);
		r.spec.should.have.property("baz", Boolean);
	});
	it("should start with the combined rows of its bases", function(done) {
		baseA.insert.apply(baseA, valsA);
		baseB.insert.apply(baseB, valsB);
		process.nextTick(function() {
			r = relvar.union(baseA, baseB);
			r.rows.should.have.length(valsA.length + valsB.length);
			done();
		});
	});
	it("should add a row when its first base adds a row", function(done) {
		this.timeout(100);
		baseB.insert.apply(baseB, valsB.concat([function() {
			r = relvar.union(baseA, baseB);
			r.on("insert", function(rows) {
				rows.should.have.length(valsA.length);
				done();
			});
			baseA.insert.apply(baseA, valsA);
		}]));
	});
	it("should add a row when its second base adds a row", function(done) {
		this.timeout(100);
		baseA.insert.apply(baseA, valsA.concat([function() {
			r = relvar.union(baseA, baseB);
			r.on("insert", function(rows) {
				rows.should.have.length(valsB.length);
				done();
			});
			baseB.insert.apply(baseB, valsB);
		}]));
	});
});