-------------------------create core tables
---results
create table core.results(
	"fixture.id" varchar PRIMARY KEY NOT NULL,
	"kickoff_date" date NOT NULL,
	"kickoff_time" time NOT NULL,
	"teams.home.name" text NOT NULL,
	"teams.away.name" text NOT NULL,
	"goals.home" smallint NOT NULL,
	"goals.away" smallint NOT NULL,
	"score.halftime.home" smallint NOT NULL,
	"score.halftime.away" smallint NOT NULL,
	"score.fulltime.home" smallint NOT NULL,
	"score.fulltime.away" smallint NOT NULL,
	"ETL_update_time" timestamp NOT NULL
);
drop table core.fixtures
---fixtures
create table core.fixtures(
	"fixture.id" varchar NOT NULL,
	"kickoff_date" date NOT NULL,
	"kickoff_time" time NOT NULL,
	"teams.home.name" text NOT NULL,
	"teams.away.name" text NOT NULL,
	"ETL_update_time" timestamp NOT NULL
);
---standings
create table core.standings (
	"rank" smallint NOT NULL,
	club text PRIMARY KEY NOT NULL,
	"MP" smallint NOT NULL,
	"W" smallint NOT NULL,
	"D" smallint NOT NULL,
	"L" smallint NOT NULL,
	"GF" smallint NOT NULL,
	"GA" smallint NOT NULL,
	"GD" smallint NOT NULL,
	"PTS" smallint NOT NULL,
	last_5 varchar NOT NULL,
	"ETL_update_time" timestamp NOT NULL
);

---top_scorers
create table core.top_scorers(
	"player.name" varchar NOT NULL,
	"player.firstname" varchar NOT NULL,
	"player.lastname" varchar NOT NULL,
	"team.name" text NOT NULL,
	"games.appearences" smallint NOT NULL,
	"goals.total" smallint NOT NULL,
	"ETL_update_time" timestamp NOT NULL
);

---top_assisters

create table core.top_assisters(
	"player.name" varchar NOT NULL,
	"player.firstname" varchar NOT NULL,
	"player.lastname" varchar NOT NULL,
	"team.name" text NOT NULL,
	"games.appearences" smallint NOT NULL,
	"goals.assists" smallint NOT NULL,
	"ETL_update_time" timestamp NOT NULL
);

select * from core.top_assisters







