import kagglehub
import logging
from pyfaktory import Client, Consumer, Job, Producer
import pandas as pd
import os
import re

kpop_df = pd.read_csv('/home/jleone5/Data-Collection/datasets/kpop_artists.csv')

kpop_names = kpop_df['Stage.Name'].tolist()
kpop_groups = kpop_df['Group'].tolist()
kpop_groups = [group for group in kpop_groups if group not in ['HIGHLIGHT', 'IVE']]

pop_df = pd.read_csv('/home/jleone5/Data-Collection/datasets/popular_artists.csv')
pop_group_artists = pop_df[pop_df['type'] == 'group']['artist'].tolist()
pop_individual_artists = pop_df[pop_df['type'] != 'group']['artist'].tolist()

class ArtistsDataset:


    def firstname_in_text(name, text):
        text = text.lower()
        name_parts = name.split()

        # Helper function to match a word boundary
        def word_in_text(word, text):
            pattern = r'\b' + re.escape(word.lower()) + r'\b'
            return bool(re.search(pattern, text))

        # Check first name as a standalone word
        if word_in_text(name_parts[0], text):
            return name
        # Check the full name as a standalone match
        elif word_in_text(name, text):
            return name
        else:
            return None

    def name_find(name, text):
        pattern = r'\b' + re.escape(name.lower()) + r'\b'
        return bool(re.search(pattern, text))

    def find_names(text):
        text = text.lower()
        found_kpop_names = [name for name in kpop_names if ArtistsDataset.name_find(name, text)]
        found_kpop_groups = [name for name in kpop_groups if ArtistsDataset.name_find(name, text)]
        found_pop_names = [name for name in pop_individual_artists if ArtistsDataset.firstname_in_text(name, text)]
        found_pop_groups = [name for name in pop_group_artists if ArtistsDataset.name_find(name, text)]
        all_found_names = found_kpop_groups + found_kpop_names + found_pop_names + found_pop_groups
        return all_found_names
    

