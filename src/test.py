import vaex


team = vaex.open("csv/team.csv")
team_details = vaex.open("csv/team_details.csv")
team_history = vaex.open("csv/team_history.csv")



pbp = (
    vaex.open("csv/play_by_play.csv")
    .groupby("player1_name", agg=[vaex.agg.count("player1_name")])
    .filter("player1_name")
    .sort("player1_name_count", ascending=False)
)


print(pbp)
