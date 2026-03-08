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


# --- 2. WORKER LOGIC ---
def process_chunk(chunk_id, data_chunk):
    """CPU-bound task performed in a separate process."""
    try:
        # Simulated failure for testing
        if chunk_id == 3:
            raise ValueError("Data corruption detected!")

        # Perform calculation
        calc = np.mean(np.sqrt(np.square(data_chunk)) * np.log1p(np.abs(data_chunk)))
        return {"id": chunk_id, "result": calc, "success": True}
    
    except Exception as e:
        return {"id": chunk_id, "error": str(e), "success": False}