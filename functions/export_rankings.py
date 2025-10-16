def export_rankings_to_file(poll_dfs, output_dir="./output"):
    """Save parsed rankings data to parquet files."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    for poll_name, df in poll_dfs.items():
        file_path = f"{output_dir}/rankings_{poll_name.lower()}.parquet"
        df.to_parquet(file_path, index=False)
        print(f"💾 Saved: {file_path}")
