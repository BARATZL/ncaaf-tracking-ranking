-- bt_schema.sql (MotherDuck compatible)
CREATE SCHEMA IF NOT EXISTS bt;

-- 1. Team Stats
CREATE TABLE IF NOT EXISTS bt.team_stats
(team_id INTEGER PRIMARY KEY,
    
    -- Game counts
    games_played INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    losses INTEGER NOT NULL,
    ties INTEGER DEFAULT 0,
    win_pct FLOAT,
    
    -- Offensive statistics (averages)
    avg_points_scored FLOAT,
    avg_total_yards FLOAT,
    avg_third_eff FLOAT,
    avg_fourth_eff FLOAT,
    avg_yards_per_pass FLOAT,
    avg_yards_per_rush FLOAT,
    avg_turnovers FLOAT,
    avg_fumbles_lost FLOAT,
    avg_ints_thrown FLOAT,
    avg_top FLOAT,  -- Time of possession
    
    -- Defensive statistics (what opponents did against this team)
    avg_points_allowed FLOAT,
    avg_yards_allowed FLOAT,
    avg_third_eff_allowed FLOAT,
    avg_fourth_eff_allowed FLOAT,
    avg_yards_per_pass_allowed FLOAT,
    avg_yards_per_rush_allowed FLOAT,
    avg_turnovers_forced FLOAT,
    avg_fumbles_forced FLOAT,
    avg_interceptions FLOAT,
    
    -- Derived efficiency metrics
    point_differential FLOAT,
    yard_differential FLOAT,
    turnover_margin FLOAT,
    points_per_yard_offense FLOAT,
    points_per_yard_defense FLOAT,
    
    -- Totals (for reference)
    total_points_scored INTEGER,
    total_points_allowed INTEGER,
    total_yards_gained INTEGER,
    total_yards_allowed INTEGER,
    
    -- Home/Away splits
    avg_points_home FLOAT,
    avg_points_away FLOAT,
    
    -- Metadata
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Pairwise Comparisons
CREATE TABLE IF NOT EXISTS bt.pairwise_comparisons (
    game_id INT PRIMARY KEY,
    home_team_id INT NOT NULL,
    away_team_id INT NOT NULL,
    home_won INT NOT NULL,  -- 1 if home team won, 0 otherwise
    home_score INT,
    away_score INT,
    score_margin INT,  -- home_score - away_score
    home_total_yards INT,
    away_total_yards INT,
    home_third_eff FLOAT,
    away_third_eff FLOAT,
    home_fourth_eff FLOAT,
    away_fourth_eff FLOAT,
    home_yards_per_pass FLOAT,
    away_yards_per_pass FLOAT,
    home_yards_per_rush FLOAT,
    away_yards_per_rush FLOAT,
    home_turnovers INT,
    away_turnovers INT
);

-- 3. Rankings
CREATE TABLE IF NOT EXISTS bt.rankings (
    team_id INT NOT NULL,
    rank INT,
    strength FLOAT NOT NULL,
    prob_vs_avg FLOAT NOT NULL,  -- Probability of beating average team
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(team_id, updated_at)
);

-- 4. Model Runs
CREATE TABLE IF NOT EXISTS bt.model_ranking_history (
    team_id INT NOT NULL,
    rank INT,
    strength FLOAT NOT NULL,
    prob_vs_avg FLOAT NOT NULL,  -- Probability of beating average team
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(team_id, updated_at)
);

-- 5. benchmarked team
CREATE TABLE IF NOT EXISTS bt.benchmark_stats (
    model_run_timestamp TIMESTAMP NOT NULL PRIMARY KEY,
    
    -- Benchmark strength (typically 0 after centering)
    benchmark_strength FLOAT DEFAULT 0,
    
    -- Average statistics across all teams
    avg_points_per_game FLOAT,
    avg_yards_per_game FLOAT,
    avg_yards_per_pass FLOAT,
    avg_yards_per_rush FLOAT,
    avg_turnovers_per_game FLOAT,
    
    -- Spread/variance measures
    std_dev_strength FLOAT,
    strength_25th_percentile FLOAT,
    strength_50th_percentile FLOAT,  -- Median
    strength_75th_percentile FLOAT,
    
    -- Min/Max for reference
    min_strength FLOAT,
    max_strength FLOAT,
    
    -- Context
    num_teams INTEGER,
    season_year INTEGER,
    week_number INTEGER,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Indexes for Performance
-- ============================================================================

-- bt.team_stats indexes
CREATE INDEX IF NOT EXISTS idx_team_stats_win_pct 
    ON bt.team_stats(win_pct DESC);

CREATE INDEX IF NOT EXISTS idx_team_stats_point_diff 
    ON bt.team_stats(point_differential DESC);

CREATE INDEX IF NOT EXISTS idx_team_stats_updated 
    ON bt.team_stats(updated_at DESC);

-- bt.benchmark_stats indexes
CREATE INDEX IF NOT EXISTS idx_benchmark_timestamp 
    ON bt.benchmark_stats(model_run_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_benchmark_season 
    ON bt.benchmark_stats(season_year, week_number);

CREATE INDEX IF NOT EXISTS idx_rankings_timestamp ON bt.rankings(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_rankings_rank ON bt.rankings(rank);
CREATE INDEX IF NOT EXISTS idx_pairwise_teams ON bt.pairwise_comparisons(home_team_id, away_team_id);