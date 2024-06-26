import pandas as pd

def main():
    dataPath = './data/spotify/track_features_with_track_id.csv'
    basePath = './data/processed/song_features_with_header.tsv'
    interactionPath = './data/processed/lfm_child_10.tsv'
    validSongs = set(pd.read_csv(interactionPath, names=['a','b','c'], delimiter='\t')['b'])
    batch_size = 100000

    process_base_dataset(basePath, dataPath, batch_size, validSongs)
    process_dataset('./data/processed/song_features.tsv', basePath, batch_size)
    process_dataset('./data/processed/song_features_no_tempo.tsv', basePath, batch_size, 'Tempo')
    process_dataset('./data/processed/song_features_no_loudness.tsv', basePath, batch_size, 'Loudness')
    process_dataset('./data/processed/song_features_no_mode.tsv', basePath, batch_size, 'Mode')
    process_dataset('./data/processed/song_features_no_timesig.tsv', basePath, batch_size, 'TimeSignature')

def process_base_dataset(savePath, dataPath, batch_size, validSongs):
    for i, chunk in enumerate(pd.read_csv(dataPath, delimiter=',', chunksize=batch_size)):
        chunk.columns = ['TrackID', 
                         'ItemID', 'Dnc', 'Eng', 'Ins', 'Acu', 
                         'Tempo', 'Vlc', 'Key', 'Lvl', 
                         'Loudness', 
                         'Mode', 'Spc', 
                         'TimeSignature']
        chunk.drop(columns=['ItemID', 'Dnc', 'Eng', 'Ins', 'Acu', 'Vlc', 'Key', 'Lvl', 'Spc'], inplace=True) 
        # df.assign(var1=df.var1.str.split(",")).explode("var1")
        chunk = chunk.assign(TrackID=chunk['TrackID'].str.replace('[','').str.replace(']','').str.split(', ')).explode('TrackID').astype(int)
        chunk = chunk.assign(Tempo=(chunk['Tempo'].astype(float) * 1000).astype(int))
        chunk = chunk.assign(Loudness=(chunk['Loudness'].astype(float) * 1000).astype(int))

        chunk = chunk[(chunk['TrackID'].isin(validSongs))]
        
        chunk.to_csv(savePath, sep='\t', index=False, header=(i==0), mode='a')
    



def process_dataset(savePath, dataPath, batch_size, drop=''):

    for i, chunk in enumerate(pd.read_csv(dataPath, delimiter='\t', chunksize=batch_size)):
        chunk.columns = ['TrackID', 
                         'Tempo', 
                         'Loudness', 
                         'Mode', 
                         'TimeSignature']
        if drop != '':
            chunk.drop(columns=[drop], inplace=True) 
        
        chunk.to_csv(savePath, sep='\t', index=False, header=False, mode='a')

main()