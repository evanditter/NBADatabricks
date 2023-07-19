# Databricks notebook source
# MAGIC %sql
# MAGIC select 
# MAGIC   *
# MAGIC from nba_team_win
# MAGIC limit 100
# MAGIC -- where total_rebounds < 0 or assists < 0 or points < 0 or turnovers < 0 or blocks < 0 or steals < 0 or personal_fouls < 0 
# MAGIC -- or offensive_rebounds < 0 or defensive_rebounds < 0 or personal_fouls_per_game < 0 or turnovers_per_game < 0 or blocks_per_game < 0 or assists_per_game < 0 or steals_per_game < 0 or total_rebounds_per_game < 0 or defensive_rebounds_per_game < 0 or offensive_rebounds_per_game < 0 or points_per_game < 0 or free_throw_pct < 0 or free_throws_attempted < 0 or free_throws < 0 or effective_field_goal_pct < 0 or two_point_pct < 0 or two_pointers_attempted < 0 or two_pointers < 0 or three_point_pct < 0 or three_pointers < 0 or three_pointers_attempted < 0 or field_goal_pct < 0 or field_goals < 0 or field_goals_attempted < 0 or minutes_played < 0 or games_played < 0 or games_started < 0 or age < 17 or age > 50

# COMMAND ----------

import unittest
from pyspark.sql.functions import col, sum, lower
from pyspark.sql.types import StructType, StringType, DoubleType
import datetime

today = datetime.date.today()

year_num = today.year
print(year_num)

player_stats_df = spark.read.table("player_stats")
draft_history_df = spark.read.table("draft_history")
nba_team_win_df = spark.read.table("nba_team_win")

test = nba_team_win_df.select(col("winning_pct")).where( (nba_team_win_df.winning_pct < 0 ) | (nba_team_win_df.winning_pct > 100) )

display(test)

# COMMAND ----------

import unittest
from pyspark.sql.functions import col, sum, lower
from pyspark.sql.types import StructType, StringType, DoubleType
import datetime

today = datetime.date.today()

year_num = today.year

player_stats_df = spark.read.table("player_stats")
draft_history_df = spark.read.table("draft_history")
nba_team_win_df = spark.read.table("nba_team_win")

class TestSilverTables(unittest.TestCase):
    def test_player_stats(self):
        assert player_stats_df.select(col("points")).where( player_stats_df.points < 0 ).isEmpty()
        assert player_stats_df.select(col("total_rebounds")).where( player_stats_df.total_rebounds < 0 ).isEmpty()
        assert player_stats_df.select(col("assists")).where( player_stats_df.assists < 0 ).isEmpty()
        assert player_stats_df.select(col("turnovers")).where( player_stats_df.turnovers < 0 ).isEmpty()
        assert player_stats_df.select(col("blocks")).where( player_stats_df.blocks < 0 ).isEmpty()
        assert player_stats_df.select(col("steals")).where( player_stats_df.steals < 0 ).isEmpty()

    def test_draft_history(self):
        assert draft_history_df.select(col("draft_year")).where( (draft_history_df.draft_year < 1946) | (draft_history_df.draft_year > year_num + 1) ).isEmpty()
        assert draft_history_df.select(col("draft_round")).where( draft_history_df.draft_round < 0 ).isEmpty()
        assert draft_history_df.select(col("round_pick")).where( (draft_history_df.round_pick < 0 ) | (draft_history_df.round_pick > 30) ).isEmpty()
        assert draft_history_df.select(col("pick_number")).where( (draft_history_df.pick_number < 0)  | (draft_history_df.pick_number > 300)).isEmpty()
        assert draft_history_df.select(col("player_id")).where( draft_history_df.player_id < 0 ).isEmpty()


    def test_nba_team_win_df(self):
        assert nba_team_win_df.select(col("season")).where( (nba_team_win_df.season < 1946) | (nba_team_win_df.season > year_num + 1) ).isEmpty()
        assert nba_team_win_df.select(col("winning_pct")).where( (nba_team_win_df.winning_pct < 0) | (nba_team_win_df.winning_pct > 100) ).isEmpty()

test_results = unittest.main(argv=[''], verbosity=2, exit=False)
assert test_results.result.wasSuccessful(), 'Test Failed; see logs above'

# COMMAND ----------


