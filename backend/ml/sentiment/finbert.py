import logging
from functools import lru_cache
from typing import Literal

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

logger = logging.getLogger(__name__)

MODEL_NAME = "ProsusAI/finbert"
SentimentLabel = Literal["positive", "negative", "neutral"]


@lru_cache(maxsize=1)
def _load_model():
    logger.info(f"Loading FinBERT model: {MODEL_NAME}")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    model.eval()
    return tokenizer, model


def score_texts(texts: list[str], batch_size: int = 16) -> list[float]:
    """
    Score a list of texts using FinBERT.
    Returns a list of sentiment scores in range [-1, 1].
    Positive = bullish, Negative = bearish, 0 = neutral.
    """
    if not texts:
        return []

    tokenizer, model = _load_model()
    scores = []
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        inputs = tokenizer(
            batch, padding=True, truncation=True, max_length=512, return_tensors="pt"
        )
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)
            probs = torch.softmax(outputs.logits, dim=-1).cpu().numpy()

        # FinBERT label order: positive=0, negative=1, neutral=2
        for prob in probs:
            score = float(prob[0] - prob[1])  # positive - negative
            scores.append(score)

    return scores
