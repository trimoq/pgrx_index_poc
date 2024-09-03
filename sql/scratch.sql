CREATE TABLE foo (
	a	int4,
	b	int4
);

CREATE EXTENSION my_extension;
SELECT foo();


CREATE FUNCTION foohandler_wrapper(internal)
RETURNS index_am_handler
AS '$libdir/my_extension'
LANGUAGE C;

-- -- Access method
CREATE ACCESS METHOD foo TYPE INDEX HANDLER foohandler_wrapper;
COMMENT ON ACCESS METHOD foo IS 'foo index access method';

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING foohandler_wrapper AS
	OPERATOR	1	=(int4, int4),
	FUNCTION	1	hashint4(int4);


insert into foo values (1,11);
insert into foo values (2,12);
insert into foo values (3,13);

CREATE INDEX fooidx ON foo USING foo (a, b);