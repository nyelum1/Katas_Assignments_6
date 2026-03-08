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
    


# --- 3. DRIVER ---
def run_pipeline():
    chunks = get_dataset()
    results = []
    
    print(f"Starting parallel processing ({NUM_CHUNKS} chunks)...")

    with ProcessPoolExecutor() as executor:
        # Dispatch tasks
        futures = {executor.submit(process_chunk, i, chunk): i for i, chunk in enumerate(chunks)}

        # Track progress as they complete
        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            print(f"Progress: {i}/{NUM_CHUNKS} chunks processed.")

    # Final Aggregation
    successful_results = [r['result'] for r in results if r['success']]
    errors = [r for r in results if not r['success']]

    # Output Summary
    if errors:
        print("\n--- Errors Encountered ---")
        for e in errors:
            print(f"Chunk {e['id']}: {e['error']}")

    if successful_results:
        final_mean = np.mean(successful_results)
        print(f"\n✅ Final Aggregated Result: {final_mean:.4f}")


if __name__ == "__main__":
    run_pipeline()