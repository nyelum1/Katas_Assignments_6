# Git Bisect 

The bisect process involves a "divide and conquer" strategy to isolate a specific failure point in a dataset or code history. By repeatedly splitting the data into halves and testing each segment, you reduce the search space logarithmically rather than linearly.

In this implementation, the process revealed that unindenting run_pipeline function was causing a failure due to an intentional error trap. By isolating this specific segment, I confirmed the error and fixed it. 