
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd 
import numpy as np
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime,timedelta, date

# Databricks notebook source
client_id = 'your_spotify_clientid' #use azure key vault with databricks scope to safely improt keys
client_secret = 'your_spotify_clientsecret'



class DataGatherer:
    def __init__(self,playlist_link):
        
        self.client_id = client_id
        self.client_secret = client_secret
        self.client_credentials_manager = SpotifyClientCredentials(client_id=self.client_id,
                                                                  client_secret=self.client_secret)
        self.spotify = spotipy.Spotify(client_credentials_manager=self.client_credentials_manager)
        playlist_URI = playlist_link.split('/')[-1].split('?')[0]
        self.data  = self.spotify.playlist_tracks(playlist_URI)

    def gather_album_data(self):
        album_list = []
        for row in self.data['items']:
            album_id = row['track']['album']['id']
            album_name = row['track']['album']['name']
            album_release_date = row['track']['album']['release_date']
            album_total_tracks = row['track']['album']['total_tracks']
            album_url = row['track']['album']['external_urls']['spotify']
            album_element = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                             'total_tracks': album_total_tracks, 'url': album_url}
            album_list.append(album_element)
        
        album_df = pd.DataFrame.from_dict(album_list)
        return album_df
    
    def gather_artist_data(self):
        artist_list = []
        for row in self.data['items']:
            for key, value in row.items():
                if key == 'track':
                    for artist in value['artists']:
                        artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'],
                                      'external_url': artist['href']}
                        artist_list.append(artist_dict)
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        return artist_df
    
    def gather_song_data(self):
        song_list = []
        for row in self.data['items']:
            track_uri = row["track"]["uri"]
            song_id = row['track']['id']
            song_name = row['track']['name']
            song_duration = row['track']['duration_ms']
            song_url = row['track']['external_urls']['spotify']
            song_popularity = row['track']['popularity']
            song_added = row['added_at']
            album_id = row['track']['album']['id']
            artist_id = row['track']['album']['artists'][0]['id']
            song_element = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url,
                            'popularity': song_popularity, 'song_added': song_added, 'album_id': album_id,
                            'artist_id': artist_id, 'uri': track_uri}
            song_list.append(song_element)
        
        song_df = pd.DataFrame.from_dict(song_list)
        return song_df

    def gather_audio_features(self):
        self.song_df = self.gather_song_data()
        audio_features_list = []

        for uri in self.song_df['uri']:
            # Extract the track ID from the URI
            track_id = uri.split(':')[-1]

            # Make an API request to get audio features for the track
            audio_features = self.spotify.audio_features(tracks=[track_id])

            if audio_features:
                audio_features_list.append(audio_features[0])
        
        audio_features_df = pd.DataFrame(audio_features_list)
        return audio_features_df
    
    
    def merge_dataframes(self, source):
        album_df = self.gather_album_data()
        artist_df = self.gather_artist_data()
        song_df = self.gather_song_data()
        audio_features_df = self.gather_audio_features()

        # Merge album with song using album_id
        df = pd.merge(album_df, song_df, on='album_id')

        # Merge artist with the above using artist_id
        df = pd.merge(df, artist_df, on='artist_id')

        audio_features_df = audio_features_df.rename(columns={'id': 'song_id'})

        # Merge audio features with song_id
        df = pd.merge(df, audio_features_df, on='song_id')

        # Drop duplicates and rename columns
        df = df.drop_duplicates(subset=['song_id'])
        df = df.drop(columns=['uri_y', 'duration_ms_y', 'url_y'])
        df = df.rename(columns={'uri_x': 'uri', 'duration_ms_x': 'duration_ms', 'url_x': 'url'})
        df['source'] = source

        return df
    


playlist_links = [
    ("https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF", 'top_50_wkly_glb'),
    ("https://open.spotify.com/playlist/37i9dQZEVXbLp5XoPON0wI",'top_50_wkly_us'),
    ("https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF", 'top_50_daily_glb'),
    ("https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp", 'top_50_daily_us'),
    ("https://open.spotify.com/playlist/37i9dQZEVXbLiRSasKsNU9", 'top_50_viral_glb'),
    ("https://open.spotify.com/playlist/37i9dQZEVXbKuaTI1Z1Afx", 'top_50_viral_us')
]

data_frames = []

for playlist_link, source in playlist_links:
    data_gatherer = DataGatherer(playlist_link)
    df = data_gatherer.merge_dataframes(source)
    data_frames.append(df)

# Concatenate all DataFrames
final_df = pd.concat(data_frames, ignore_index=True)
final_df = final_df.rename(columns={'name':'album_name'})


# for new releases
client_credentials_manager = SpotifyClientCredentials(client_id=client_id,
                                                                  client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
new_releases = sp.new_releases(limit=50) 

album_ids = []

# Iterate through the top 50 new releases
for i in range(1, 50):  # Loop from 1 to 50
    album_id = new_releases['albums']['items'][i]['id']
    album_ids.append(album_id)
df = pd.DataFrame({'album_id': album_ids})

final_df['new_release'] =  np.where(final_df['album_id'].isin(df['album_id']), 1, 0)



#get artist urls from get tracks
artist = final_df['artist_id'].unique().tolist()

artist_data = []

for artist in artist:
    art = sp.artist(artist)
    artist_id = artist
    artist_url = art['images'][0]['url']
    artist_data.append({'artist_id':artist_id, 'artist_image_url':artist_url})

artist_data = pd.DataFrame(artist_data)
final_df = pd.merge(final_df,artist_data, on ='artist_id')



#write output file to blob storage using azure-storage py library
#below set of instructions delete the files from the previous run and uploads the new file with the timestamp suffixed to the filename

output = final_df.to_csv(index=False,sep='\t',encoding='UTF-8') # save file as tab seperated

current_date = date.today().strftime('%Y-%m-%d')

# Get the current date
current_date = date.today()

# get date of one day ago
one_day_ago = current_date - timedelta(days=1)

def blob_azure(filename,operation):
    CONTAINERNAME = 'spotifydata'
    BLOBNAME = f'spotify_data_{filename}.csv'
    STORAGEACCOUNTURL= 'your_blob_storage_accoount_url'
    STORAGEACCOUNTKEY= 'your_storage_account_key'
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    
    if operation == 'delete':
        blob_client_instance.delete_blob()
    else:
        blob_client_instance.upload_blob(output,blob_type="BlockBlob",overwrite=True)

blob_azure(one_day_ago,'delete') #delete existing file in storage

blob_azure(current_date,'upload') #upload data from final_df



