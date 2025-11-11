INSERT OR REPLACE INTO ncaa.bt.pairwise_comparisons
SELECT
    h.game_id,
    h.team_id AS home_team_id,
    a.team_id AS away_team_id,
    CASE WHEN h.score > a.score THEN 1 ELSE 0 END AS home_won,
    h.score AS home_score,
    a.score AS away_score,
    h.score - a.score AS score_margin,
    h.total_yards AS home_total_yards,
    a.total_yards AS away_total_yards,
    h.third_eff AS home_third_eff,
    a.third_eff AS away_third_eff,
    h.fourth_eff AS home_fourth_eff,
    a.fourth_eff AS away_fourth_eff,
    h.yards_per_pass AS home_yards_per_pass,
    a.yards_per_pass AS away_yards_per_pass,
    h.yards_per_rush AS home_yards_per_rush,
    a.yards_per_rush AS away_yards_per_rush,
    h.turnovers AS home_turnovers,
    a.turnovers AS away_turnovers
FROM ncaa.real_deal.fact_game_team h
JOIN ncaa.real_deal.fact_game_team a 
    ON h.game_id = a.game_id
WHERE h.home_away = 'home'
  AND a.home_away = 'away'