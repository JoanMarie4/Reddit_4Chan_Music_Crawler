import kagglehub
import logging
from pyfaktory import Client, Consumer, Job, Producer
import pandas as pd
import os

#KPOP idols dataset
k_path = kagglehub.dataset_download("faisalamir/kpop-idol-followers")
csv_path = os.path.join(k_path, "kpop_idol_followers.csv")
df = pd.read_csv(csv_path)

#separating dataset based on gender
boys_df = df[df['Gender.x'] == 'Boy']
girls_df = df[df['Gender.x'] == 'Girl']


#top males from dataset
top_100_boy = boys_df.sort_values(by='Followers', ascending=False).head(100)
#top females from dataset
top_100_girl = girls_df.sort_values(by='Followers', ascending=False).head(100)

#combining together into kpop dataset
##kpop_artist_dataset.to_csv("kpop_artists.csv", index=False)


#Top Spotify Artists Dataset
p_path = kagglehub.dataset_download("jackharding/spotify-artist-metadata-top-10k")
csv_path_two = os.path.join(p_path, "top10k-spotify-artist-metadata.csv")
df_two = pd.read_csv(csv_path_two)

print(df.columns)

#separating database based on gender
male_df = df_two[df_two['gender'] == 'male']
female_df = df_two[df_two['gender'] == 'female']
mixed_df = df_two[df_two['gender'] == 'mixed']

#top males from database
top_100_male = male_df.sort_values(by='Unnamed: 0', ascending=True).head(100)
#top females from database
top_100_female = female_df.sort_values(by='Unnamed: 0', ascending=True).head(100)

#assigned gender to all boy or all girl groups
top_50_mixed = mixed_df.sort_values(by='Unnamed: 0', ascending=True).head(50)
top_50_mixed.loc[0:202, 'gender'] = 'male'
top_50_mixed.at[155, 'gender'] = 'female'
top_50_mixed.at[202, 'gender'] = 'female'
added_indexes = [14, 27, 32, 48, 49, 51, 71, 85, 90, 108, 117, 124, 125, 129, 145, 147, 154, 155, 158, 166, 173, 177, 181, 197, 202]
groups_df = top_50_mixed.loc[added_indexes]

#popular_artists_dataset = pd.concat([top_100_male, top_100_female, groups_df], ignore_index=True)
#popular_artists_dataset.to_csv("popular_artists.csv", index=False)
print(df_two.columns)

'''
manually added 2025 Grammy artist nominations that are not already in list
- P Diddy/Sean Combs (not nominated just culturally relevant)
- Chappell Roan
- The Beatles
- Jacob Collier
- Andre 3000
- Shaboozey
- Benson Boone
- Doechii
- Raye
- Teddy Swims
- Gracie Abrams
- Madison Beer
- Troye Sivan
- Coco Jones
- Childish Gambino
- Usher
- Morgan Wallen

'''

'''
print("Top 100 Male Artistss")
print(top_100_male[['index', 'artist']])

print("\nTop 100 Female Artistss")
print(top_100_female[['index', 'artist']])

print("\nGoups w/ Gender:")
print(groups_df[['index', 'artist', 'gender']])
'''