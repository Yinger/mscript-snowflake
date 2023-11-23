-- Drop all procedures within the given schema
---------------------------------------------------------------------------------------------------------
-- create procedure (sql)
CREATE OR REPLACE PROCEDURE lytest_drop_proc_all_sql(
    procedure_schema VARCHAR
)
RETURNS TABLE (proc VARCHAR)
LANGUAGE SQL
--EXECUTE AS CALLER
AS
BEGIN
    let schema VARCHAR := :procedure_schema;
    let res RESULTSET := (
        with cte 
        as (select PROCEDURE_CATALOG , PROCEDURE_SCHEMA, PROCEDURE_NAME, split (trim (ARGUMENT_SIGNATURE, '()'),',') as array 
        from INFORMATION_SCHEMA.PROCEDURES
        where
                PROCEDURE_SCHEMA = :schema
            ),
        array_values as (
        select max (PROCEDURE_CATALOG) as PROCEDURE_CATALOG
          , max(PROCEDURE_SCHEMA) as PROCEDURE_SCHEMA
          , PROCEDURE_NAME
          , array_agg(split_part(ltrim(d.value,' '),' ',2)) as filtered_values
        from cte, 
        lateral Flatten(array) d
        group by array, PROCEDURE_NAME)
        
        select PROCEDURE_CATALOG||'.'||PROCEDURE_SCHEMA||'.'|| PROCEDURE_NAME||'(' ||replace (trim (filtered_values::varchar,'[]'),'"', '' )||')' as proc
        from array_values
    );
    
    RETURN TABLE(res);
END;

---------------------------------------------------------------------------------------------------------
-- create procedure (js)
CREATE OR REPLACE PROCEDURE lytest_drop_proc_all(
    procedure_schema VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function _do_select(p_sql, p_params) {
        try {
            var rs = snowflake.execute({sqlText:p_sql, binds: p_params});
            rs.next();
            return rs.getColumnValue(1);
        } catch (err) {
            throw err;
        }
    }

    var schema = PROCEDURE_SCHEMA;
    var json_result = new Array();

    var sqlVars = new Array();
    sqlVars.push(schema);
    var script1 = "CALL lytest_drop_proc_all_sql(:1)";
    try {
        var rs = snowflake.execute({sqlText:script1, binds: sqlVars});
        while(rs.next())
        {
            try{
                var procName = rs.getColumnValue(1);
                var sql_command = 'DROP PROCEDURE ' + procName + ';';
                snowflake.execute({sqlText:sql_command});
                json_result.push('dropped : ' + procName);
            }
            catch (err)  {
                json_result.push('Error: ' + err.message);
           } 
        }
    } catch (err) {
        throw err;
    }
    return JSON.stringify(json_result);
$$;

---------------------------------------------------------------------------------------------------------
call lytest_drop_proc_all('PUBLIC');
