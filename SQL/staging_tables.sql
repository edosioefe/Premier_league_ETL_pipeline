-------------------create staging tables-----------------------
---results
create table staging.results(
	"fixture.id" varchar,
	"kickoff_date" varchar,
	"kickoff_time" varchar,
	"teams.home.name" varchar,
	"teams.away.name" varchar,
	"goals.home" varchar,
	"goals.away" varchar,
	"score.halftime.home" varchar,
	"score.halftime.away" varchar,
	"score.fulltime.home" varchar,
	"score.fulltime.away" varchar
	);
---fixtures
create table staging.fixtures(
	"fixture.id" varchar,
	"kickoff_date" varchar,
	"kickoff_time" varchar,
	"teams.home.name" varchar,
	"teams.away.name" varchar
);
---standings
create table staging.standings (
	"rank" varchar,
	club varchar,
	"MP" varchar,
	"W" varchar,
	"D" varchar,
	"L" varchar,
	"GF" varchar,
	"GA" varchar,
	"GD" varchar,
	"PTS" varchar,
	"last 5" varchar
);

---top_scorers
create table staging.top_scorers(
	"player.name" varchar,
	"player.firstname" varchar,
	"player.lastname" varchar,
	"team.name" varchar,
	"games.appearences" varchar,
	"goals.total" varchar
);

---top_assisters
create table staging.top_assisters(
	"player.name" varchar,
	"player.firstname" varchar,
	"player.lastname" varchar,
	"team.name" varchar,
	"games.appearences" varchar,
	"goals.assists" varchar
);



	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
		












































































