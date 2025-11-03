"""
Copyright 2024 Kyle Luoma

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from transformers import AutoTokenizer, CanineForSequenceClassification, pipeline
import torch
import pandas as pd
import sqlite3
from util import tokenprocessing as tp


class CanineIdentifierClassifier:
    """
    A classifier for identifying word naturalness using a pre-trained text analysis model.
    Classifies words as Regular (label N1), Low (label N2), or Least (label N3) natural.
    Attributes:
        model_name (str): The name of the model used for classification.
        checkpoint (int): The checkpoint number of the model.
        id2label (dict): A dictionary mapping label IDs to label names.
        label2id (dict): A dictionary mapping label names to label IDs.
        classifier (pipeline): The sentiment analysis pipeline used for classification.
        identifiers (pd.DataFrame): A DataFrame containing identifiers to classify.
    Methods:
        __init__(identifiers=pd.DataFrame()):
            Initializes the classifier with the given identifiers DataFrame.
        do_batch_job(ident_df=None, save_as_excel=False, make_tag=True):
            Performs batch classification on the given DataFrame of identifiers.
            Args:
                ident_df (pd.DataFrame, optional): The DataFrame of identifiers to classify. Defaults to None.
                save_as_excel (bool, optional): Whether to save the results as an Excel file. Defaults to False.
                make_tag (bool, optional): Whether to add a token tag to the text before classification. Defaults to True.
        classify_identifier(identifier, make_tag=True):
            Classifies a single identifier.
            Args:
                identifier (str): The identifier to classify.
                make_tag (bool, optional): Whether to add a token tag to the identifier before classification. Defaults to True.
            Returns:
                list: The classification result.
    """


    def __init__(self, identifiers = pd.DataFrame()):
        
        self.model_name = "kyleluoma/SNAILS-word-naturalness-classifier"
        self.checkpoint = 5590
        self.id2label = {0: "N1", 1: "N2", 2: "N3"}
        self.label2id = {"N1": 0, "N2": 1, "N3": 2}
        self.classifier = pipeline(
            "sentiment-analysis", 
            model = "kyleluoma/SNAILS-word-naturalness-classifier",
            device=0
            )
        self.identifiers = identifiers
        self.cache_folder = "./util/naturalness_cache"
        self.cache_db_conn = sqlite3.connect(f"{self.cache_folder}/cache_db.sqlite")
        self.init_cache_db()

    def init_cache_db(self):
        new_table_query = """
    CREATE TABLE IF NOT EXISTS identifier_naturalness(
        identifier varchar UNIQUE, 
        label varchar,
        score float
        )
    """
        cursor = self.cache_db_conn.cursor()
        cursor.execute(new_table_query)
        self.cache_db_conn.commit()


    def insert_level_into_cache(self, identifier: str, label: str, score: float):
        insert_query = """
        INSERT OR REPLACE INTO identifier_naturalness (identifier, label, score)
        VALUES (?, ?, ?)
        """
        cursor = self.cache_db_conn.cursor()
        cursor.execute(insert_query, (identifier, label, score))
        self.cache_db_conn.commit()


    def retrieve_level_from_cache_db(self, identifier: str) -> str:
        retrieval_query = f"""
    SELECT label, score FROM identifier_naturalness WHERE identifier='{identifier}'
"""
        cursor = self.cache_db_conn.cursor()
        result = cursor.execute(retrieval_query)
        row = result.fetchone()
        if row is None:
            return None
        else:
            return (row[0], row[1])
        

    def do_batch_job(self, ident_df: pd.DataFrame = None, save_as_excel: bool = False, make_tag: bool = True):
        """
        Processes a batch of text data through a classifier and optionally saves the results to an Excel file.
        Args:
            ident_df (pd.DataFrame, optional): DataFrame containing the text data to be classified. 
                               If None, uses self.identifiers. Defaults to None.
            save_as_excel (bool, optional): If True, saves the results to an Excel file. Defaults to False.
            make_tag (bool, optional): If True, appends a token tag to the text before classification. Defaults to True.
        Returns:
            None
        """

        auto_scores = []

        if ident_df == None:
            ident_df = self.identifiers

        for row in ident_df.itertuples():
            if make_tag:
                pred = classifier(row.text + tp.make_token_tag(row.text))
            else:
                pred = self.classifier(row.text)
            print(pred)
            auto_scores.append(pred[0]['label'])

        ident_df["prediction"] = auto_scores

        if save_as_excel:
            ident_df[['text', 'prediction', 'category']].to_excel(
                f"./classifier-inference-results/{self.model_name}-cp-{self.checkpoint}.xlsx",
                index=False
                )
    
    def classify_identifier(self, identifier: str, make_tag: bool = True):
        """
        Classifies the given identifier using the classifier.
        Args:
            identifier (str): The identifier to classify.
            make_tag (bool, optional): If True, appends a token tag to the identifier before classification. Defaults to True.
        Returns:
            The classification result of the identifier.
        """

        identifier = str(identifier)
        cached_pred = self.retrieve_level_from_cache_db(identifier)
        if cached_pred != None:
            return [{"label": cached_pred[0], "score": cached_pred[1]}]
        if make_tag:
            identifier += (" " + tp.make_token_tag(identifier))
        pred = self.classifier(identifier)
        self.insert_level_into_cache(identifier, pred[0]["label"], pred[0]["score"])
        # print("Classifying", identifier, "as", pred)
        return pred


if __name__ == "__main__":
    classifier = CanineIdentifierClassifier()
    print(classifier.classify_identifier("WinterWeather"))
    print(classifier.classify_identifier("WintrWthr"))
    print(classifier.classify_identifier("WWth"))
