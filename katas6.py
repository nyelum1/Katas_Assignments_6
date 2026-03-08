import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed



# --- CONFIGURATION ---
NUM_ROWS = 1_000_000
NUM_CHUNKS = 10