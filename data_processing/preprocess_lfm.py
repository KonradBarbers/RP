import sys
import pandas as pd
import os

dataset = sys.argv[1]
users_file = './data/lfm2b/raw/LFM-2b/users.tsv.bz2'  # Update this path to the actual location of your users.tsv.bz2 file

def main():   
    if dataset == "lfm":
        savePath = './data/processed/lfm_child_10.tsv'
        dataPath = './data/lfm2b/raw/LFM-2b/listening-counts.tsv.bz2'
        batch_size = 100000
        if not os.path.exists('processed'):
            os.makedirs('processed')
        
    elif dataset == "ml":
        savePath = './data/processed/ml/ml_full.tsv'
        dataPath = './data/lfm2b/raw/ml-1m/ratings.dat'
        batch_size = 10000
        if not os.path.exists('./data/processed/ml'):
            os.makedirs('./data/processed/ml')

    # Filter users under 18
    user_ids = filter_users_under_18(users_file)

    process_dataset(savePath, dataPath, batch_size, dataset, user_ids)
    
def filter_users_under_18(users_file):
    users = pd.read_csv(users_file, delimiter='\t', compression='bz2')
    # Filter out users with age under 18 and age under 0
    valid_users = users[(users['age'] >= 0) & (users['age'] < 18)]
    return set(valid_users['user_id'])

def process_dataset(savePath, dataPath, batch_size, dataset, user_ids):    
    print('Loading and processing batches...')
    delimiter = '\t' if dataset == "lfm" else '::'
    compression='bz2' if dataset == "lfm" else None

    # Iterate over the raw dataset in chunks
    for i, chunk in enumerate(pd.read_csv(dataPath, delimiter=delimiter, chunksize=batch_size, compression=compression)):
        if i % 10 == 0:  # Print status every 10 chunks
            print(f'Processed {i * batch_size} rows; current chunk size: {len(chunk)}')

        if dataset == 'lfm':
            chunk.columns = ['UserID', 'ItemID', 'Rating']
            # Filter for users under 18 and interactions with a minimal playcount of 10
            chunk = chunk[(chunk['UserID'].isin(user_ids)) & (chunk['Rating'] >= 10)]
            
        elif dataset == "ml":
            chunk.columns = ['UserID', 'ItemID', 'Rating', 'Timestamp']
            chunk.drop(columns=['Timestamp'], inplace=True)
            chunk['Rating'] = chunk['Rating'].astype(int)
            # Filter for users under 18 and interactions with a minimal playcount of 10
            chunk = chunk[(chunk['UserID'].isin(user_ids)) & (chunk['Rating'] >= 10)]

        chunk.to_csv(savePath, sep='\t', index=False, header=False, mode='a')

    print('Processing complete.')

if __name__ == '__main__':
    main()
