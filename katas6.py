import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed



# --- CONFIGURATION ---
NUM_ROWS = 1_000_000
NUM_CHUNKS = 10


# --- 1. DATA PREP ---
def get_dataset():
    """Generates or reads the dataset."""
    print(f"Generating {NUM_ROWS:,} rows of data...")
    df = pd.DataFrame(np.random.randn(NUM_ROWS, 1), columns=['value'])
    return np.array_split(df, NUM_CHUNKS)