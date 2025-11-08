
-- sql/schema/bt_schema.sql
-- Comprehensive schema for Bradley-Terry ranking system
-- All tables are under the 'bt' schema for organization

CREATE SCHEMA IF NOT EXISTS bt;

-- ============================================================================
-- Core Bradley-Terry Tables
-- ============================================================================

-- Table 1: Team season statistics aggregated from game-level data
-- This table stores offensive and defensive statistics for each team
CREATE TABLE IF NOT EXISTS bt.team_stats (
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
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key to source data
  --  FOREIGN KEY (team_id) REFERENCES real_deal.dim_teams(team_id)
);

-- Table 2: Pairwise game comparisons for Bradley-Terry model input
-- Each row represents one game with both teams' performance
CREATE TABLE IF NOT EXISTS bt.pairwise_comparisons (
    comparison_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    game_id INTEGER NOT NULL,
    team_a_id INTEGER NOT NULL,
    team_b_id INTEGER NOT NULL,
    
    -- Game outcome
    team_a_wins INTEGER NOT NULL CHECK (team_a_wins IN (0, 1)),
    team_a_score INTEGER NOT NULL,
    team_b_score INTEGER NOT NULL,
    score_differential INTEGER,  -- team_a_score - team_b_score
    
    -- Team A game statistics
    team_a_total_yards INTEGER,
    team_a_third_eff FLOAT,
    team_a_fourth_eff FLOAT,
    team_a_yards_per_pass FLOAT,
    team_a_yards_per_rush FLOAT,
    team_a_turnovers INTEGER,
    team_a_fumbles_lost INTEGER,
    team_a_ints_thrown INTEGER,
    team_a_top INTEGER,
    
    -- Team B game statistics
    team_b_total_yards INTEGER,
    team_b_third_eff FLOAT,
    team_b_fourth_eff FLOAT,
    team_b_yards_per_pass FLOAT,
    team_b_yards_per_rush FLOAT,
    team_b_turnovers INTEGER,
    team_b_fumbles_lost INTEGER,
    team_b_ints_thrown INTEGER,
    team_b_top INTEGER,
    
    -- Match context
    team_a_home_away VARCHAR(10) CHECK (team_a_home_away IN ('home', 'away', 'neutral')),
    team_b_home_away VARCHAR(10) CHECK (team_b_home_away IN ('home', 'away', 'neutral')),
    is_rivalry_game BOOLEAN DEFAULT FALSE,
    is_conference_game BOOLEAN DEFAULT FALSE,
    
    -- Week information (for time-weighted models)
    week_number INTEGER,
    game_date DATE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(game_id, team_a_id, team_b_id),
    CHECK (team_a_id < team_b_id),  -- Ensure each game appears once
  --  FOREIGN KEY (game_id) REFERENCES real_deal.dim_games(game_id),
  -- FOREIGN KEY (team_a_id) REFERENCES real_deal.dim_teams(team_id),
  --  FOREIGN KEY (team_b_id) REFERENCES real_deal.dim_teams(team_id)
);

-- Table 3: Bradley-Terry model rankings output
-- Stores the results of each model run
CREATE TABLE IF NOT EXISTS bt.rankings (
    ranking_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    team_id INTEGER NOT NULL,
    
    -- Ranking information
    rank INTEGER NOT NULL,
    bt_strength FLOAT NOT NULL,  -- The core BT strength parameter
    bt_strength_std_error FLOAT,  -- Standard error of strength estimate
    prob_beat_benchmark FLOAT NOT NULL CHECK (prob_beat_benchmark >= 0 AND prob_beat_benchmark <= 1),
    
    -- Confidence intervals (optional)
    strength_ci_lower FLOAT,
    strength_ci_upper FLOAT,
    
    -- Team record at time of ranking
    wins INTEGER,
    losses INTEGER,
    ties INTEGER,
    
    -- Model information
    model_run_timestamp TIMESTAMP NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_type VARCHAR(50) DEFAULT 'standard',  -- 'standard', 'with_covariates', 'time_weighted'
    
    -- Model parameters used
    home_field_advantage_param FLOAT,
    covariate_weights JSONB,  -- Store any covariate coefficients as JSON
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
  --  FOREIGN KEY (team_id) REFERENCES real_deal.dim_teams(team_id),
  --  UNIQUE(team_id, model_run_timestamp, model_version)
);

-- Table 4: Model run metadata and diagnostics
-- Tracks each model execution with performance metrics
CREATE TABLE IF NOT EXISTS bt.model_runs (
    run_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Model configuration
    model_version VARCHAR(50) NOT NULL,
    model_type VARCHAR(50) NOT NULL,
    
    -- Input data stats
    num_teams INTEGER NOT NULL,
    num_games INTEGER NOT NULL,
    num_comparisons INTEGER NOT NULL,
    date_range_start DATE,
    date_range_end DATE,
    
    -- Model performance metrics
    log_likelihood FLOAT,
    convergence_status VARCHAR(50),
    num_iterations INTEGER,
    optimization_time_seconds FLOAT,
    
    -- Validation metrics
    prediction_accuracy FLOAT,  -- % of games predicted correctly
    brier_score FLOAT,  -- Probabilistic prediction quality
    calibration_score FLOAT,
    
    -- Comparison with other rankings (if available)
    correlation_with_ap_poll FLOAT,
    correlation_with_coaches_poll FLOAT,
    correlation_with_cfp FLOAT,
    
    -- Execution metadata
    airflow_run_id VARCHAR(255),
    airflow_dag_id VARCHAR(255),
    execution_status VARCHAR(50) DEFAULT 'running',
    error_message TEXT,
    
    -- Metadata
    created_by VARCHAR(100) DEFAULT 'system',
    notes TEXT,
    
    UNIQUE(run_timestamp, model_version)
);

-- Table 5: Head-to-head matchup predictions
-- Store predicted probabilities for any team matchup
CREATE TABLE IF NOT EXISTS bt.matchup_predictions (
    prediction_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    model_run_id INTEGER NOT NULL,
    
    -- Teams involved
    team_a_id INTEGER NOT NULL,
    team_b_id INTEGER NOT NULL,
    
    -- Predictions
    prob_team_a_wins FLOAT NOT NULL CHECK (prob_team_a_wins >= 0 AND prob_team_a_wins <= 1),
    prob_team_b_wins FLOAT NOT NULL CHECK (prob_team_b_wins >= 0 AND prob_team_b_wins <= 1),
    expected_point_spread FLOAT,  -- Positive means team_a favored
    
    -- Context adjustments
    home_team_id INTEGER,  -- Which team is home (if applicable)
    neutral_site BOOLEAN DEFAULT FALSE,
    
    -- Strength differential
    strength_differential FLOAT,  -- team_a_strength - team_b_strength
    
    -- If this was an actual game, store the result
    actual_game_id INTEGER,
    actual_winner_id INTEGER,
    prediction_correct BOOLEAN,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    --FOREIGN KEY (model_run_id) REFERENCES bt.model_runs(run_id),
    --FOREIGN KEY (team_a_id) REFERENCES real_deal.dim_teams(team_id),
    --FOREIGN KEY (team_b_id) REFERENCES real_deal.dim_teams(team_id),
    --FOREIGN KEY (home_team_id) REFERENCES real_deal.dim_teams(team_id),
    --FOREIGN KEY (actual_game_id) REFERENCES real_deal.dim_games(game_id),
    --FOREIGN KEY (actual_winner_id) REFERENCES real_deal.dim_teams(team_id),
    UNIQUE(model_run_id, team_a_id, team_b_id)
);

-- Table 6: Ranking history for trend analysis
-- Denormalized table optimized for time-series queries
CREATE TABLE IF NOT EXISTS bt.ranking_history (
    history_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    team_id INTEGER NOT NULL,
    model_run_timestamp TIMESTAMP NOT NULL,
    
    -- Ranking at this point in time
    rank INTEGER NOT NULL,
    bt_strength FLOAT NOT NULL,
    prob_beat_benchmark FLOAT NOT NULL,
    
    -- Changes from previous week
    rank_change INTEGER,  -- Positive = moved up
    strength_change FLOAT,
    
    -- Record at time
    wins INTEGER,
    losses INTEGER,
    
    -- Reference to full ranking record
    ranking_id INTEGER NOT NULL,
    
    -- Metadata
    week_number INTEGER,
    season_year INTEGER,
    
    --FOREIGN KEY (team_id) REFERENCES real_deal.dim_teams(team_id),
    --FOREIGN KEY (ranking_id) REFERENCES bt.rankings(ranking_id),
    
    UNIQUE(team_id, model_run_timestamp)
);

-- Table 7: Benchmark team statistics
-- Track the "average team" benchmark over time
CREATE TABLE IF NOT EXISTS bt.benchmark_stats (
    benchmark_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    model_run_timestamp TIMESTAMP NOT NULL,
    
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
    strength_75th_percentile FLOAT,
    
    -- Metadata
    num_teams INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(model_run_timestamp)
);

-- ============================================================================
-- Indexes for Performance
-- ============================================================================

-- bt.team_stats indexes
CREATE INDEX IF NOT EXISTS idx_team_stats_win_pct 
    ON bt.team_stats(win_pct DESC);
CREATE INDEX IF NOT EXISTS idx_team_stats_point_diff 
    ON bt.team_stats(point_differential DESC);

-- bt.pairwise_comparisons indexes
CREATE INDEX IF NOT EXISTS idx_pairwise_game_id 
    ON bt.pairwise_comparisons(game_id);
CREATE INDEX IF NOT EXISTS idx_pairwise_teams 
    ON bt.pairwise_comparisons(team_a_id, team_b_id);
CREATE INDEX IF NOT EXISTS idx_pairwise_date 
    ON bt.pairwise_comparisons(game_date DESC);
CREATE INDEX IF NOT EXISTS idx_pairwise_week 
    ON bt.pairwise_comparisons(week_number);

-- bt.rankings indexes
CREATE INDEX IF NOT EXISTS idx_rankings_timestamp 
    ON bt.rankings(model_run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_rankings_team_timestamp 
    ON bt.rankings(team_id, model_run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_rankings_rank 
    ON bt.rankings(rank) WHERE model_run_timestamp = (SELECT MAX(model_run_timestamp) FROM bt.rankings);
CREATE INDEX IF NOT EXISTS idx_rankings_strength 
    ON bt.rankings(bt_strength DESC);

-- bt.model_runs indexes
CREATE INDEX IF NOT EXISTS idx_model_runs_timestamp 
    ON bt.model_runs(run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_model_runs_status 
    ON bt.model_runs(execution_status);

-- bt.matchup_predictions indexes
CREATE INDEX IF NOT EXISTS idx_matchup_model_run 
    ON bt.matchup_predictions(model_run_id);
CREATE INDEX IF NOT EXISTS idx_matchup_teams 
    ON bt.matchup_predictions(team_a_id, team_b_id);
CREATE INDEX IF NOT EXISTS idx_matchup_actual_game 
    ON bt.matchup_predictions(actual_game_id) WHERE actual_game_id IS NOT NULL;

-- bt.ranking_history indexes
CREATE INDEX IF NOT EXISTS idx_history_team 
    ON bt.ranking_history(team_id, model_run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_history_timestamp 
    ON bt.ranking_history(model_run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_history_rank 
    ON bt.ranking_history(rank);

-- bt.benchmark_stats indexes
CREATE INDEX IF NOT EXISTS idx_benchmark_timestamp 
    ON bt.benchmark_stats(model_run_timestamp DESC);

-- ============================================================================
-- Comments for Documentation
-- ============================================================================

COMMENT ON SCHEMA bt IS 'Bradley-Terry ranking system schema for college football';

COMMENT ON TABLE bt.team_stats IS 'Aggregated season statistics for each team, used as input for Bradley-Terry model';
COMMENT ON TABLE bt.pairwise_comparisons IS 'Pairwise game comparisons in format required for Bradley-Terry model fitting';
COMMENT ON TABLE bt.rankings IS 'Bradley-Terry model output with team rankings and probabilities';
COMMENT ON TABLE bt.model_runs IS 'Metadata and diagnostics for each model execution';
COMMENT ON TABLE bt.matchup_predictions IS 'Predicted probabilities for team matchups';
COMMENT ON TABLE bt.ranking_history IS 'Historical rankings for trend analysis and visualization';
COMMENT ON TABLE bt.benchmark_stats IS 'Statistics for the benchmark "average team" used in ranking calculations';

COMMENT ON COLUMN bt.rankings.bt_strength IS 'Bradley-Terry strength parameter (log-odds scale), centered at 0 for average team';
COMMENT ON COLUMN bt.rankings.prob_beat_benchmark IS 'Probability this team would beat the benchmark average team';
COMMENT ON COLUMN bt.pairwise_comparisons.team_a_wins IS 'Binary outcome: 1 if team A won, 0 if team B won';
