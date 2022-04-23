DROP FUNCTION IF EXISTS select_by_year;
CREATE OR REPLACE FUNCTION select_by_year(year_to_fetch integer)
    RETURNS TABLE(region varchar(500), max_score numeric(4,1))
    AS $$
    BEGIN
         RETURN QUERY
             select regname, max(physball100) from zno_data
             where physteststatus = 'Зараховано' and year = year_to_fetch group by regname;
    END; $$
    LANGUAGE plpgsql;


select first_result.region, first_result.max_score as "2021", second_result.max_score as "2019"
from select_by_year(2021) as first_result
left join select_by_year(2019) as second_result on first_result.region = second_result.region;