-- bt_schema.sql (MotherDuck compatible)
CREATE SCHEMA IF NOT EXISTS bt;

-- 1. Team Stats
team_id INTEGER PRIMARY KEY,
    
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
    game_id INTEGER NOT NULL,
    team_a_id INTEGER NOT NULL,
    team_b_id INTEGER NOT NULL,
    team_a_wins INTEGER NOT NULL CHECK (team_a_wins IN (0, 1)),
    team_a_score INTEGER NOT NULL,
    team_b_score INTEGER NOT NULL,
    score_differential INTEGER,
    team_a_home_away VARCHAR(10),
    team_b_home_away VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(game_id, team_a_id, team_b_id),
    CHECK (team_a_id < team_b_id)
);

-- 3. Rankings
CREATE TABLE IF NOT EXISTS bt.rankings (
    team_id INTEGER NOT NULL,
    model_run_timestamp TIMESTAMP NOT NULL,
    rank INTEGER NOT NULL,
    bt_strength FLOAT NOT NULL,
    prob_beat_benchmark FLOAT NOT NULL CHECK (prob_beat_benchmark >= 0 AND prob_beat_benchmark <= 1),
    wins INTEGER,
    losses INTEGER,
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(team_id, model_run_timestamp)
);

-- 4. Model Runs
CREATE TABLE IF NOT EXISTS bt.model_runs (
    run_timestamp TIMESTAMP NOT NULL PRIMARY KEY,
    model_version VARCHAR(50) NOT NULL,
    num_teams INTEGER NOT NULL,
    num_games INTEGER NOT NULL,
    convergence_status VARCHAR(50),
    execution_status VARCHAR(50) DEFAULT 'running'
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

CREATE INDEX IF NOT EXISTS idx_rankings_timestamp ON bt.rankings(model_run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_rankings_rank ON bt.rankings(rank);
CREATE INDEX IF NOT EXISTS idx_pairwise_teams ON bt.pairwise_comparisons(team_a_id, team_b_id);