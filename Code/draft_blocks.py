
def load_dataframes(results_dir):
    df_blocks_full = pd.read_csv(os.path.join(results_dir, "df_blocks_full.csv"))
    return df_blocks_full

def debug_functions(df_direct_pool, df_blocks_full, interval_dataframes, sample_hashid=None):

    if sample_hashid is not None:
        print(df_blocks_full[df_blocks_full['hashid'] == sample_hashid])
        print(df_direct_pool[df_direct_pool.index == sample_hashid])

        for hash, intervals_dict in interval_dataframes['same'].items():
            if hash == sample_hashid:
                print("SAME")
                for interval_debug, interval_dict_debug in intervals_dict.items():
                    df_interval_debug = interval_dict_debug['df']
                    print(interval_debug)
                    print(df_interval_debug)

        for hash, intervals_dict in interval_dataframes['other'].items():
            if hash == sample_hashid:
                print("OTHER")
                for interval_debug, interval_dict_debug in intervals_dict.items():
                    df_interval_debug = interval_dict_debug['df']
                    print(interval_debug)
                    print(df_interval_debug)


    # NOT MAINTAINED - would need to update if needed
    # df_blocks_full = load_dataframes(results_dir)
    # debug_functions(df_direct_pool, df_blocks_full, interval_dataframes_pool, sample_hashid=4214004393)