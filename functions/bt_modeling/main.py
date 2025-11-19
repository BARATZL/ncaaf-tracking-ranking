import functions_framework
import duckdb
import pandas as pd
import numpy as np
from choix import ilsr_pairwise
import os
from google.cloud import secretmanager
import networkx as nx


project_id = 'baratz00-ba882-fall25'
secret_id = 'MotherDuck'
version_id = 'latest'
bucket_name = "ba882-ncaa-project"

@functions_framework.http
def bradley_terry_rankings(request):
    """
    Runs Bradley-Terry model on pairwise comparisons with margin-of-victory weighting
    """
    try:
        print("Starting Bradley-Terry rankings function...")
        
        # Get MotherDuck token
        print("Accessing Secret Manager...")
        sm = secretmanager.SecretManagerServiceClient()
        secret_name = f'projects/{project_id}/secrets/{secret_id}/versions/{version_id}'
        response = sm.access_secret_version(request={"name": secret_name})
        md_token = response.payload.data.decode("UTF-8")
        print("✓ Successfully retrieved MotherDuck token")
        
        # Connect to MotherDuck
        print("Connecting to MotherDuck...")
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        print("✓ Connected to MotherDuck")
        
        # 1. Pull pairwise comparisons with statistics
        print("Fetching pairwise comparisons from database...")
        df = md.execute("""
            SELECT 
                home_team_id,
                away_team_id,
                home_won,
                home_score,
                away_score,
                score_margin,
                home_total_yards,
                away_total_yards,
                home_turnovers,
                away_turnovers
            FROM ncaa.bt.pairwise_comparisons
        """).df()
        print(f"✓ Retrieved {len(df)} games")
        print(f"Sample data:\n{df.head()}")
        
        # 2. Get unique teams and create team index mapping
        print("\nCreating team mappings...")
        all_teams = pd.concat([df['home_team_id'], df['away_team_id']]).unique()
        team_to_idx = {team: idx for idx, team in enumerate(all_teams)}
        idx_to_team = {idx: team for team, idx in team_to_idx.items()}
        n_teams = len(all_teams)
        print(f"✓ Found {n_teams} unique teams")
        
        # 3. Convert team IDs to indices and apply margin-of-victory weighting
        print("\nBuilding weighted comparison list...")
        comparisons = []
        
        for _, row in df.iterrows():
            home_idx = team_to_idx[row['home_team_id']]
            away_idx = team_to_idx[row['away_team_id']]
            
            # Calculate weight based on margin of victory
            # Use a logarithmic scale to prevent blowouts from dominating
            margin = abs(row['score_margin']) if pd.notna(row['score_margin']) else 0
            
            # Weight function: log scale with cap at 3x
            # This means: 7pt win ≈ 1x, 14pt ≈ 1.5x, 21pt ≈ 2x, 35pt+ ≈ 3x
            if margin > 0:
                weight = min(1 + np.log1p(margin) / 3, 3.0)
            else:
                weight = 1.0  # Default weight if no score data
            
            # Determine winner and add weighted comparisons
            if row['home_won'] == 1:
                winner_idx = home_idx
                loser_idx = away_idx
            else:
                winner_idx = away_idx
                loser_idx = home_idx
            
            # Add multiple copies of the comparison based on weight
            # Round to nearest integer, minimum of 1
            num_copies = max(1, round(weight))
            comparisons.extend([(winner_idx, loser_idx)] * num_copies)
        
        print(f"✓ Created {len(comparisons)} weighted comparisons from {len(df)} games")
        print(f"Average weight per game: {len(comparisons) / len(df):.2f}x")
        print(f"First 5 comparisons: {comparisons[:5]}")
        
        # 4. Filter teams by minimum games played
        print("\nFiltering teams by minimum games played...")
        team_game_count = {}
        for winner, loser in comparisons:
            team_game_count[winner] = team_game_count.get(winner, 0) + 1
            team_game_count[loser] = team_game_count.get(loser, 0) + 1

        # Note: game count is now based on weighted comparisons
        # Adjust threshold accordingly (multiply by average weight)
        avg_weight = len(comparisons) / len(df)
        min_games_weighted = int(4 * avg_weight)  # Equivalent to ~4 actual games
        
        eligible_teams = {team for team, count in team_game_count.items() if count >= min_games_weighted}
        print(f"✓ {len(eligible_teams)} teams have played at least ~4 games (weighted threshold: {min_games_weighted})")
        print(f"  Filtered out {len(team_game_count) - len(eligible_teams)} teams")

        # Filter comparisons to only include eligible teams
        filtered_comparisons = [
            (w, l) for w, l in comparisons 
            if w in eligible_teams and l in eligible_teams
        ]
        print(f"✓ Filtered to {len(filtered_comparisons)} comparisons between eligible teams")
        
        # 5. Check connectivity
        print("\nChecking graph connectivity...")
        G = nx.Graph()
        for winner_idx, loser_idx in filtered_comparisons:
            G.add_edge(winner_idx, loser_idx)
        
        print(f"Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
        print(f"Number of connected components: {nx.number_connected_components(G)}")
        
        # Get all connected components and their sizes
        components = list(nx.connected_components(G))
        component_sizes = [len(c) for c in components]
        print(f"Component sizes: {sorted(component_sizes, reverse=True)}")
        
        # Get the largest connected component
        largest_cc = max(components, key=len)
        print(f"✓ Largest connected component has {len(largest_cc)} teams")
        
        # 6. Filter to largest connected component
        print("\nFiltering to largest connected component...")
        final_comparisons = [
            (w, l) for w, l in filtered_comparisons 
            if w in largest_cc and l in largest_cc
        ]
        print(f"✓ Final dataset: {len(final_comparisons)} weighted comparisons")
        
        # 7. Remap team indices to be contiguous (0 to n-1)
        print("\nRemapping team indices...")
        connected_teams = sorted(list(largest_cc))
        new_team_to_idx = {old_idx: new_idx for new_idx, old_idx in enumerate(connected_teams)}
        idx_to_team_connected = {new_idx: idx_to_team[old_idx] for old_idx, new_idx in zip(connected_teams, range(len(connected_teams)))}
        
        remapped_comparisons = [(new_team_to_idx[w], new_team_to_idx[l]) for w, l in final_comparisons]
        print(f"✓ Remapped {len(remapped_comparisons)} comparisons")
        print(f"Number of teams in connected component: {len(connected_teams)}")
        print(f"First 5 remapped comparisons: {remapped_comparisons[:5]}")
        
        # Verify remapped comparisons are valid
        max_idx = max(max(w, l) for w, l in remapped_comparisons)
        print(f"Max index in remapped comparisons: {max_idx}")
        print(f"Expected max index: {len(connected_teams) - 1}")
        
        # 8. Check for teams with perfect records (for informational purposes only)
        print("\nChecking for teams with perfect records...")
        team_wins = {}
        team_losses = {}
        for winner, loser in remapped_comparisons:
            team_wins[winner] = team_wins.get(winner, 0) + 1
            team_losses[loser] = team_losses.get(loser, 0) + 1

        all_team_indices = set(range(len(connected_teams)))
        teams_only_winning = [t for t in all_team_indices if t in team_wins and t not in team_losses]
        teams_only_losing = [t for t in all_team_indices if t in team_losses and t not in team_wins]

        print(f"Teams with only wins: {len(teams_only_winning)}")
        print(f"Teams with only losses: {len(teams_only_losing)}")

        if teams_only_winning:
            print(f"  Undefeated teams (indices): {teams_only_winning[:10]}")
        if teams_only_losing:
            print(f"  Winless teams (indices): {teams_only_losing[:10]}")
        
        # Note: We're NOT adding regularization - margin weighting handles this naturally
        print("✓ Using weighted comparisons only (no artificial regularization)")
        
        # 9. Run Bradley-Terry model
        print("\n📊 Running Bradley-Terry model with margin-of-victory weighting...")
        print(f"Input: {len(connected_teams)} teams, {len(remapped_comparisons)} weighted comparisons")
        
        try:
            log_params = ilsr_pairwise(
                len(connected_teams), 
                remapped_comparisons,
                alpha=0.01  # Small regularization for numerical stability
            )
            print("✓ Bradley-Terry model completed successfully!")
        except Exception as bt_error:
            print(f"Standard method failed: {bt_error}")
            print("Trying dense matrix approach...")
            from choix import ilsr_pairwise_dense
            
            # Create win matrix
            win_matrix = np.zeros((len(connected_teams), len(connected_teams)))
            for winner, loser in remapped_comparisons:
                win_matrix[winner, loser] += 1
            
            print(f"Win matrix shape: {win_matrix.shape}")
            print(f"Total comparisons in matrix: {win_matrix.sum()}")
            
            log_params = ilsr_pairwise_dense(win_matrix, alpha=0.01)
            print("✓ Bradley-Terry model completed with dense method!")
        
        strengths = np.exp(log_params)
        print(f"✓ Calculated {len(strengths)} team strengths")
        print(f"Strength stats: min={strengths.min():.4f}, max={strengths.max():.4f}, mean={strengths.mean():.4f}")
        
        # 10. Calculate average team strength
        avg_strength = np.mean(strengths)
        print(f"\nAverage team strength: {avg_strength:.4f}")
        
        # 11. Calculate win probability vs average team for each team
        print("\nCalculating win probabilities...")
        win_probs = []
        for new_idx, strength in enumerate(strengths):
            team_id = idx_to_team_connected[new_idx]
            prob_vs_avg = strength / (strength + avg_strength)
            win_probs.append({
                'team_id': int(team_id),
                'strength': float(strength),
                'prob_vs_avg': float(prob_vs_avg)
            })
        
        print(f"✓ Calculated probabilities for {len(win_probs)} teams")
        
        # 12. Convert to DataFrame
        print("\nCreating DataFrame...")
        win_probs_df = pd.DataFrame(win_probs)
        print(f"✓ DataFrame shape: {win_probs_df.shape}")
        print(f"DataFrame preview:\n{win_probs_df.head()}")
        
        # Add timestamp
        win_probs_df['updated_at'] = pd.Timestamp.now()
        
        # 13. Sort by prob_vs_avg and assign ranks
        print("\nSorting and ranking...")
        win_probs_df = win_probs_df.sort_values('prob_vs_avg', ascending=False)
        win_probs_df['rank'] = range(1, len(win_probs_df) + 1)
        print(f"✓ Assigned ranks to all teams")
        print(f"Top 5 teams:\n{win_probs_df.head()}")
        
        # 14. Insert into database
        print("\nInserting results into database history...")
        md.execute("""
            INSERT INTO ncaa.bt.model_ranking_history 
            SELECT 
                team_id,
                rank,
                strength,
                prob_vs_avg,
                updated_at
            FROM win_probs_df
        """)

        print(f"✓ Inserted {len(win_probs_df)} records into ncaa.bt.model_ranking_history")
        
        md.execute("""
            CREATE OR REPLACE TABLE ncaa.bt.rankings AS (
            SELECT 
                team_id,
                rank,
                strength,
                prob_vs_avg,
                updated_at
            FROM win_probs_df)
        """)
        print(f"✓ Replaced ncaa.bt.rankings with {len(win_probs_df)} current rankings")
        
        # 15. Get top 25 for response
        top_25 = win_probs_df.head(25)
        
        print("\n✅ Function completed successfully!")
        print(f"📊 Weighting resulted in {len(remapped_comparisons)}/{len(df)} = {len(remapped_comparisons)/len(df):.2f}x average weight per game")
        
        # Return results
        return {
            "status": "success",
            "top_25_teams": top_25.to_dict(orient='records'),
            "total_teams": n_teams,
            "connected_teams": len(connected_teams),
            "total_games": len(df),
            "weighted_comparisons": len(remapped_comparisons),
            "average_weight": round(len(remapped_comparisons) / len(df), 2)
        }, 200
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        print(f"Traceback:\n{traceback.format_exc()}")
        return {"error": str(e), "traceback": traceback.format_exc()}, 500
    
    finally:
        if 'md' in locals():
            md.close()
            print("Database connection closed")
