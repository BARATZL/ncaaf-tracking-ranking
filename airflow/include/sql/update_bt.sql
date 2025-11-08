-- sql/aggregate_team_stats.sql
-- Aggregate team-level statistics for Bradley-Terry model
-- Populates bt.team_stats table

CREATE OR REPLACE TABLE bt.team_stats AS
WITH team_offensive_stats AS (
    SELECT
        team_id,
        COUNT(*) as games_played,
        
        -- Offensive averages
        AVG(score) as avg_points_scored,
        AVG(total_yards) as avg_total_yards,
        AVG(third_eff) as avg_third_eff,
        AVG(fourth_eff) as avg_fourth_eff,
        AVG(yards_per_pass) as avg_yards_per_pass,
        AVG(yards_per_rush) as avg_yards_per_rush,
        AVG(turnovers) as avg_turnovers,
        AVG(fumbles_lost) as avg_fumbles_lost,
        AVG(ints_thrown) as avg_ints_thrown,
        AVG(top) as avg_top,
        
        -- Totals
        SUM(score) as total_points_scored,
        SUM(total_yards) as total_yards_gained,
        
        -- Win-loss record
        SUM(CASE WHEN t.score > opp.score THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN t.score < opp.score THEN 1 ELSE 0 END) as losses,
        SUM(CASE WHEN t.score = opp.score THEN 1 ELSE 0 END) as ties,
        
        -- Home/Away splits
        AVG(CASE WHEN home_away = 'home' THEN t.score END) as avg_points_home,
        AVG(CASE WHEN home_away = 'away' THEN t.score END) as avg_points_away
        
    FROM real_deal.fact_game_team t
    JOIN real_deal.fact_game_team opp 
        ON t.game_id = opp.game_id 
        AND t.team_id != opp.team_id
    GROUP BY team_id
),
team_defensive_stats AS (
    SELECT
        t.team_id,
        
        -- Defensive stats (what opponents did against this team)
        AVG(opp.score) as avg_points_allowed,
        AVG(opp.total_yards) as avg_yards_allowed,
        AVG(opp.third_eff) as avg_third_eff_allowed,
        AVG(opp.fourth_eff) as avg_fourth_eff_allowed,
        AVG(opp.yards_per_pass) as avg_yards_per_pass_allowed,
        AVG(opp.yards_per_rush) as avg_yards_per_rush_allowed,
        
        -- Turnovers forced
        AVG(opp.turnovers) as avg_turnovers_forced,
        AVG(opp.fumbles_lost) as avg_fumbles_forced,
        AVG(opp.ints_thrown) as avg_interceptions,
        
        -- Totals
        SUM(opp.score) as total_points_allowed,
        SUM(opp.total_yards) as total_yards_allowed
        
    FROM real_deal.fact_game_team t
    JOIN real_deal.fact_game_team opp 
        ON t.game_id = opp.game_id 
        AND t.team_id != opp.team_id
    GROUP BY t.team_id
)
SELECT
    os.team_id,
    os.games_played,
    os.wins,
    os.losses,
    os.ties,
    os.wins::FLOAT / NULLIF(os.games_played, 0) as win_pct,
    
    -- Offensive stats
    os.avg_points_scored,
    os.avg_total_yards,
    os.avg_third_eff,
    os.avg_fourth_eff,
    os.avg_yards_per_pass,
    os.avg_yards_per_rush,
    os.avg_turnovers,
    os.avg_fumbles_lost,
    os.avg_ints_thrown,
    os.avg_top,
    
    -- Defensive stats
    ds.avg_points_allowed,
    ds.avg_yards_allowed,
    ds.avg_third_eff_allowed,
    ds.avg_fourth_eff_allowed,
    ds.avg_yards_per_pass_allowed,
    ds.avg_yards_per_rush_allowed,
    ds.avg_turnovers_forced,
    ds.avg_fumbles_forced,
    ds.avg_interceptions,
    
    -- Derived metrics
    os.avg_points_scored - ds.avg_points_allowed as point_differential,
    os.avg_total_yards - ds.avg_yards_allowed as yard_differential,
    ds.avg_turnovers_forced - os.avg_turnovers as turnover_margin,
    os.avg_points_scored / NULLIF(os.avg_total_yards, 0) as points_per_yard_offense,
    ds.avg_points_allowed / NULLIF(ds.avg_yards_allowed, 0) as points_per_yard_defense,
    
    -- Totals
    os.total_points_scored,
    ds.total_points_allowed,
    os.total_yards_gained,
    ds.total_yards_allowed,
    
    -- Home/Away
    os.avg_points_home,
    os.avg_points_away,
    
    CURRENT_TIMESTAMP as updated_at
    
FROM team_offensive_stats os
JOIN team_defensive_stats ds ON os.team_id = ds.team_id
;
