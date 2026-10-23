import os

import numpy as np
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

EMBED_MODEL = "gemini-embedding-001"
CHAT_MODEL = "gemini-3.5-flash-lite"
SAMPLE_SIZE = 100
TOP_K = 3
CACHE_FILE = "review_embeddings.parquet"

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))



def embed(texts):
    # Gemini accepts a limited number of texts per call, so send them in chunks of 100
    vectors = []
    for i in range(0, len(texts), 100):
        chunk = texts[i : i + 100]
        response = client.models.embed_content(model=EMBED_MODEL, contents=chunk)
        vectors.extend([e.values for e in response.embeddings])
    return vectors


def read_reviews_from_snowflake():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    query = f"""
        SELECT REVIEW_ID, CITY, RATING, COMMENT
        FROM ZOMATO.STAGING.STG_REVIEWS
        SAMPLE ({SAMPLE_SIZE} ROWS)
    """
    df = conn.cursor().execute(query).fetch_pandas_all()
    conn.close()
    df.columns = [col.lower() for col in df.columns]
    return df.dropna(subset=["comment"])


@st.cache_data
def load_reviews():
    if os.path.exists(CACHE_FILE):
        return pd.read_parquet(CACHE_FILE)

    df = read_reviews_from_snowflake()
    df["embedding"] = embed(df["comment"].tolist())
    df.to_parquet(CACHE_FILE)
    return df


def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def find_similar_reviews(question, df):
    question_vector = embed([question])[0]

    scores = []
    for review_vector in df["embedding"]:
        scores.append(cosine_similarity(question_vector, review_vector))

    df = df.copy()
    df["score"] = scores
    return df.nlargest(TOP_K, "score")


def ask_llm(question, top_reviews):
    context = ""
    for _, row in top_reviews.iterrows():
        context += f"({row['city']}, {row['rating']} stars) {row['comment']}\n"

    response = client.models.generate_content(
        model=CHAT_MODEL,
        contents=f"Question: {question}\n\nReviews:\n{context}",
        config=types.GenerateContentConfig(
            system_instruction="Answer ONLY using the customer reviews provided."
            "Be concise. If the reviews don't cover it, say so.",
            temperature=0.2,
            max_output_tokens=300,
        ),
    )
    return response.text


st.title("Chat with your Zomato Reviews")
st.caption(f"Searching {SAMPLE_SIZE} reviews, answering with {CHAT_MODEL}")

review_df = load_reviews()

question = st.text_input(
    "Ask a question about your reviews:",
    placeholder="e.g. What are the most common complaints about delivery?",
)

if question:
    top_reviews = find_similar_reviews(question, review_df)
    st.markdown("**Answer:**")
    st.write(ask_llm(question, top_reviews))

    with st.expander("Reviews used to build this answer"):
        st.dataframe(top_reviews[["city", "rating", "comment"]], hide_index=True)