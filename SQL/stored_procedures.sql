-----------------staging to core stored procedure-----------------

---match results data from last 2 weeks coming in from staging
---once in the core the result will be kept, 
---due to 2 week duplicate data may be inserted from staging hence the on conflict do nothing
create or replace procedure "core".sp_results_staging_to_core() as
$$
Declare
	v_sqlstate text;
    v_message text;
    v_context text;
Begin
	
	
	insert into core.results("fixture.id",
	"kickoff_date",
	"kickoff_time",
	"teams.home.name",
	"teams.away.name",
	"goals.home",
	"goals.away",
	"score.halftime.home",
	"score.halftime.away",
	"score.fulltime.home",
	"score.fulltime.away",
	"ETL_update_time"
							)
	
	select "fixture.id"::varchar,
	"kickoff_date"::date,
	"kickoff_time"::time,
	"teams.home.name"::text,
	"teams.away.name"::text,
	"goals.home"::smallint,
	"goals.away"::smallint,
	"score.halftime.home"::smallint,
	"score.halftime.away"::smallint,
	"score.fulltime.home"::smallint,
	"score.fulltime.away"::smallint,
	date_trunc('second', now())::timestamp as "ETL_update_time"
	FROM staging."results"
	ON CONFLICT("fixture.id")
	DO NOTHING
	;
	
	raise notice'ETL load to core.results table complete at %',now();
    Exception
        when others then
            GET STACKED DIAGNOSTICS
                v_sqlstate = returned_sqlstate,
                v_message = message_text,
                v_context = pg_exception_context;
            RAISE NOTICE 'etl load to core.results table failed at %',now();
            RAISE NOTICE 'sqlstate: %', v_sqlstate;
            RAISE NOTICE 'message: %', v_message;
            RAISE NOTICE 'context: %', v_context;
			---ROLLBACK;
	COMMIT;    
End;
$$
Language plpgsql

---storing only scheduled future(1 week) matches
---need to remove match that have now been played; doent store that many rows so ill just truncate the table before insert 
create or replace procedure "core".sp_fixtures_staging_to_core() as
$$
Declare
	v_sqlstate text;
    v_message text;
    v_context text;
	
Begin
	TRUNCATE TABLE core.fixtures;
	insert into core.fixtures(
		"fixture.id",
		"kickoff_date",
		"kickoff_time",
		"teams.home.name",
		"teams.away.name",
		"ETL_update_time"
	)
	select 
		"fixture.id"::varchar,
		"kickoff_date"::date,
		"kickoff_time"::time,
		"teams.home.name"::text,
		"teams.away.name"::text,
		date_trunc('second',now())::timestamp as "ETL_update_time"
	FROM staging.fixtures;
	
	raise notice 'ETL load to core.fixtures table complete at %',now();
    Exception
        when others then
            GET STACKED DIAGNOSTICS
                v_sqlstate = returned_sqlstate,
                v_message = message_text,
                v_context = pg_exception_context;
            RAISE NOTICE 'etl load to core.fixtures table failed at %',now();
            RAISE NOTICE 'sqlstate: %', v_sqlstate;
            RAISE NOTICE 'message: %', v_message;
            RAISE NOTICE 'context: %', v_context;
			--ROLLBACK;
	COMMIT;    
End;
$$
Language plpgsql
		


---update standings table by truncating table and inserting new values; small table so can do this.
CREATE or replace procedure "core".sp_standings_staging_to_core() AS
$$
DECLARE
	v_sqlstate text;
    v_message text;
    v_context text;
BEGIN
	TRUNCATE TABLE core.standings;
	INSERT INTO core.standings(
		"rank",
		club,
		"MP",
		"W",
		"D",
		"L",
		"GF",
		"GA",
		"GD",
		"PTS",
		"last_5",
		"ETL_update_time"
	)
	SELECT 
		"rank" varchar,
		club::text,
		"MP"::smallint,
		"W"::smallint,
		"D"::smallint,
		"L"::smallint,
		"GF"::smallint,
		"GA"::smallint,
		"GD"::smallint,
		"PTS"::smallint,
		"last 5"::varchar as "last_5",
		date_trunc('second',now())::timestamp as "ETL_update_time"
	FROM
		staging.standings
		;
		
	RAISE NOTICE'ETL load to core.standings table complete at %',now();
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = returned_sqlstate,
                v_message = message_text,
                v_context = pg_exception_context;
            RAISE NOTICE 'etl load to core.standings table failed at %',now();
            RAISE NOTICE 'sqlstate: %', v_sqlstate;
            RAISE NOTICE 'message: %', v_message;
            RAISE NOTICE 'context: %', v_context;
			--ROLLBACK;
	COMMIT;    
End;
$$
Language plpgsql

---updated top goal scorer data coming in from staging (may only be one row untlil end of season)
---so will truncate table before insert 
CREATE or replace procedure "core".sp_top_scorers_staging_to_core() AS
$$
DECLARE
	v_sqlstate text;
    v_message text;
    v_context text;
BEGIN
	
	TRUNCATE TABLE core.top_scorers;
	INSERT INTO core.top_scorers(
		"player.name",
		"player.firstname",
		"player.lastname",
		"team.name",
		"games.appearences",
		"goals.total",
		"ETL_update_time"
	)
	SELECT 
		"player.name"::varchar,
		"player.firstname"::varchar,
		"player.lastname"::varchar,
		"team.name"::text,
		"games.appearences"::smallint,
		"goals.total"::smallint,
		date_trunc('second',now())::timestamp as "ETL_update_time"
	FROM
		staging.top_scorers;
	
		
	RAISE NOTICE 'ETL load to core.top_scorers table complete at %',now();
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = returned_sqlstate,
                v_message = message_text,
                v_context = pg_exception_context;
            RAISE NOTICE 'etl load to core.top_scorers table failed at %',now();
            RAISE NOTICE 'sqlstate: %', v_sqlstate;
            RAISE NOTICE 'message: %', v_message;
            RAISE NOTICE 'context: %', v_context;
			--ROLLBACK;
	COMMIT;    
End;
$$
Language plpgsql
	
	
---updated top goal assister data coming in from staging (may only be one row untlil end of season)
---so will truncate table before insert 	
CREATE procedure "core".sp_top_assisters_staging_to_core() AS
$$
DECLARE
	v_sqlstate text;
    v_message text;
    v_context text;
BEGIN
	
	
	TRUNCATE TABLE core.top_assisters;
	INSERT INTO core.top_assisters(
		"player.name",
		"player.firstname",
		"player.lastname",
		"team.name",
		"games.appearences",
		"goals.assists",
		"ETL_update_time"
	)
	SELECT 
		"player.name"::varchar,
		"player.firstname"::varchar,
		"player.lastname"::varchar,
		"team.name"::text,
		"games.appearences"::smallint,
		"goals.assists"::smallint,
		date_trunc('second',now())::timestamp as "ETL_update_time"
	FROM
		staging.top_assisters;
	
	
	RAISE NOTICE 'ETL load to core.top_assisters table complete at %',now();
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = returned_sqlstate,
                v_message = message_text,
                v_context = pg_exception_context;
            RAISE NOTICE 'etl load to core.top_assisters table failed at %',now();
            RAISE NOTICE 'sqlstate: %', v_sqlstate;
            RAISE NOTICE 'message: %', v_message;
            RAISE NOTICE 'context: %', v_context;
			--ROLLBACK;
	COMMIT;    
End;
$$
Language plpgsql
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	