DO $$
DECLARE
    cols text;   -- will store generated column expressions per region
    sql  text;   -- will store the final dynamic SQL
BEGIN
    -- Drop existing summary table if it exists
    EXECUTE 'DROP TABLE IF EXISTS cat_reg';

    -- Build dynamic list of SUM(...) columns, one for each region
    SELECT string_agg(
        format(
            'COALESCE(SUM(CASE WHEN region = %L THEN totalprice END),0)::numeric(12,2) AS %I',
            r.region, r.region
        ),
        ', ' ORDER BY r.region
    )
    INTO cols
    FROM (SELECT DISTINCT region FROM food_sales ORDER BY region) r;

    -- Safety check: raise error if no regions found
    IF cols IS NULL THEN
        RAISE EXCEPTION 'No regions found in food_sales';
    END IF;

    -- Build final CREATE TABLE AS SELECT with dynamic region columns
    sql := format($fmt$
        CREATE TABLE cat_reg AS
        SELECT
            category as "Category",
            %s,  -- dynamic region columns
            SUM(totalprice)::numeric(12,2) AS "Grand Total"
        FROM food_sales
        GROUP BY category
        ORDER BY category;
    $fmt$, cols);

    -- Execute dynamic SQL to create the pivoted summary table
    EXECUTE sql;

END $$;

-- Test the result: show pivoted sales by category and region
SELECT * FROM cat_reg;
