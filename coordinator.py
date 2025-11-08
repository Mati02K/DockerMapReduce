import rpyc
import collections
import time
import os
import requests
import zipfile
import sys
import concurrent.futures

# ---------- Dynamic workers ----------
# WORKERS = [
#     ("docker-worker1", 18861),
#     ("docker-worker2", 18861),
#     ("docker-worker3", 18861)
# ]

WORKER_COUNT = int(os.getenv("WORKER_COUNT", "3"))
WORKER_HOST = os.getenv("WORKER_HOST", "docker-worker")
WORKER_PORT = int(os.getenv("WORKER_PORT", "18861"))

# Build the logical list of worker 
WORKERS = [(WORKER_HOST, WORKER_PORT) for _ in range(WORKER_COUNT)]
WAIT_TIME = int(os.getenv("WAIT_TIME", "300")) # I am setting this to 300 as default despite 20 mentioned in HW bcoz for single workers it can take more time. IF you are running for multiple workers only change this otherwise code will fail.
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "8")) # I found splitting more chunks is good for performance. 
NO_OF_PARALLEL_THREADS = int(os.getenv("NO_OF_PARALLEL_THREADS", "1")) # no of parallel thread to execute for calling workers (setting it to only one, if using workers > 4 then only change or else multiple threads may lead to bottleneck)

def mapreduce_wordcount(text):
    start_time = time.time()
    """
    Executes a complete MapReduce word count job across all workers.
    1. Split text into chunks
    2. Connect to workers
    3. MAP PHASE: Send chunks in PARALLEL with retry logic
    4. SHUFFLE PHASE: Group intermediate pairs by key
    5. REDUCE PHASE: Send partitions in PARALLEL with retry logic
    6. FINAL AGGREGATION
    """
    print("[Coordinator] Starting MapReduce job...")

    # Split text into MORE chunks for better parallelism
    n_workers = len(WORKERS)
    N_CHUNKS = n_workers * CHUNK_SIZE  # better load distribution
    chunks = split_text(text, N_CHUNKS)

    # Connect to all workers
    conns = []
    for host, port in WORKERS:
        try:
            print(f"[Coordinator] Connecting to {host}:{port} ...")
            conn = rpyc.connect(host, port, config={
                "allow_pickle": True,    # to enable marshalling, kudos :- https://medium.com/@mishraarvind2222/pickle-vs-json-which-is-faster-in-python3-6b39b9010a99 
                "allow_all_attrs": True,    
                "allow_getattr": True,
                "sync_request_timeout": WAIT_TIME,
            })
            conns.append(conn)
        except Exception as e:
            print(f"[Coordinator] Failed to connect to {host}:{port} - {e}")

    if not conns:
        raise RuntimeError("[Coordinator] No active workers connected!")

    # MAP PHASE WITH PARALLEL EXECUTION
    noOfThreadPools = len(conns) * NO_OF_PARALLEL_THREADS
    print("[Coordinator] Starting MAP phase with parallel execution...")
    
    def process_chunk(args):
        """Helper function to process a chunk with retry logic"""
        chunk_idx, chunk, conns = args
        max_retries = len(conns)
        
        for retry_count in range(max_retries):
            try:
                worker_idx = (chunk_idx + retry_count) % len(conns)
                conn = conns[worker_idx]
                
                print(f"[Coordinator] Chunk {chunk_idx} -> worker-{worker_idx+1} (attempt {retry_count+1})")
                
                result = conn.root.map(chunk)
                result = rpyc.utils.classic.obtain(result)
                
                print(f"[Coordinator] Worker-{worker_idx+1} completed chunk {chunk_idx}: {len(result)} unique keys")
                return result
                
            except Exception as e:
                print(f"[Coordinator] Error on worker-{worker_idx+1} for chunk {chunk_idx}: {e}")
                if retry_count >= max_retries - 1:
                    raise RuntimeError(f"Failed to process chunk {chunk_idx} after {max_retries} attempts")
                print(f"[Coordinator] Retrying chunk {chunk_idx}...")
                time.sleep(1)
    
    # Execute all map tasks in parallel using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(noOfThreadPools) as executor:
        chunk_args = [(i, chunk, conns) for i, chunk in enumerate(chunks)]
        map_results = list(executor.map(process_chunk, chunk_args))
    
    print(f"[Coordinator] MAP phase complete. {len(map_results)} partial results gathered.")
    
    # SHUFFLE PHASE — group intermediate pairs by key
    print("[Coordinator] Shuffling intermediate key-value pairs...")
    grouped = collections.defaultdict(list)
    for partial_dict in map_results:
        for key, value in partial_dict.items():
            grouped[key].append(value)

    # REDUCE PHASE WITH PARALLEL EXECUTION
    print("[Coordinator] Starting REDUCE phase with parallel execution...")
    partitions = partition_dict(grouped, n_workers)
    
    def process_partition(args):
        """Helper function to process a partition with retry logic"""
        partition_idx, partition, conns = args
        max_retries = len(conns)
        
        for retry_count in range(max_retries):
            try:
                worker_idx = (partition_idx + retry_count) % len(conns)
                conn = conns[worker_idx]
                
                print(f"[Coordinator] Partition {partition_idx} -> worker-{worker_idx+1} (attempt {retry_count+1})")
                
                result = conn.root.reduce(partition)
                # CRITICAL FIX: Obtain the result to avoid RPyC proxy overhead
                # This single fix saved me 5x time
                result = rpyc.utils.classic.obtain(result)
                
                print(f"[Coordinator] Worker-{worker_idx+1} completed partition {partition_idx}: {len(result)} keys")
                return result
                
            except Exception as e:
                print(f"[Coordinator] Error on worker-{worker_idx+1} for partition {partition_idx}: {e}")
                if retry_count >= max_retries - 1:
                    raise RuntimeError(f"Failed to process partition {partition_idx} after {max_retries} attempts")
                print(f"[Coordinator] Retrying partition {partition_idx}...")
                time.sleep(1)
    
    # Execute all reduce tasks in parallel
    with concurrent.futures.ThreadPoolExecutor(noOfThreadPools) as executor:
        partition_args = [(i, partition, conns) for i, partition in enumerate(partitions)]
        reduced_results = list(executor.map(process_partition, partition_args))
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time: {} before Aggregation to count Bottleneck".format(elapsed_time))
    
    # FINAL AGGREGATION
    print("[Coordinator] Aggregating final results...")

    # Since each partition has unique keys (no overlap after shuffle),
    # we can simply merge all dictionaries
    final_counts = {}
    for partial in reduced_results:
        # Extra safety: in case above somehow pass by value is missed.
        if hasattr(partial, '_pyroUri') or 'rpyc' in str(type(partial)):
            partial = rpyc.utils.classic.obtain(partial)
        final_counts.update(partial)

    # Sort by count descending
    word_counts = sorted(final_counts.items(), key=lambda x: x[1], reverse=True)

    print("[Coordinator] MapReduce job completed successfully.")

    return word_counts


def split_text(text, n):
    """
    Split text into n chunks aligned to full lines, never cutting inside words.
    """
    lines = text.splitlines()
    total = len(lines)
    
    # Calculate chunk sizes (distribute remainder evenly)
    base_size = total // n
    remainder = total % n
    
    chunks = []
    start = 0
    
    for i in range(n):
        # First 'remainder' chunks get one extra line
        chunk_size = base_size + (1 if i < remainder else 0)
        end = start + chunk_size
        
        chunk = "\n".join(lines[start:end])
        chunks.append(chunk)
        start = end
    
    print(f"[Coordinator] Split into {len(chunks)} line-based chunks ({total} total lines)")
    return chunks


def partition_dict(d, n):
    """
    Partition dictionary keys into n buckets using hash function.
    """
    partitions = [collections.defaultdict(list) for _ in range(n)]
    
    for key, value in d.items():
        index = hash(key) % n
        partitions[index][key].extend(value)
    
    return partitions


def download(url):
    print(f"[Coordinator] Starting with total workers count {WORKER_COUNT}")
    
    """Downloads and unzips a wikipedia dataset in txt/."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "txt")
    os.makedirs(output_dir, exist_ok=True)
    
    filename = url.split('/')[-1]
    filepath = os.path.join(output_dir, filename)
    extracted_path = filepath.replace('.zip', '')
    
    # Skip if already extracted
    if os.path.exists(extracted_path):
        print(f"[Coordinator] Dataset already exists at {extracted_path}")
        return extracted_path
    
    # Download if missing
    if not os.path.exists(filepath):
        print(f"[Coordinator] Downloading {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[Coordinator] Downloaded to {filepath}")
    
    # Extract if it's a zip file
    if filepath.endswith('.zip'):
        print(f"[Coordinator] Extracting {filepath}...")
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        print(f"[Coordinator] Extracted to {extracted_path}")
        return extracted_path

    return filepath


def read_dataset(path):
    """
    Reads the dataset file returned by download() and returns its text content.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"[Coordinator] Dataset not found at {path}")

    print(f"[Coordinator] Reading dataset from {path}...")
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        data = f.read()

    print(f"[Coordinator] Loaded file: {path}, size: {len(data):,} characters")
    return data


if __name__ == "__main__":
    # DOWNLOAD AND UNZIP DATASET
    textPath = os.getenv("DATA_URL", "https://mattmahoney.net/dc/enwik9.zip")
    textPath = download(textPath)
    text = read_dataset(textPath)
    
    start_time = time.time()
    word_counts = mapreduce_wordcount(text)
    
    if word_counts:
        print('\nTOP 20 WORDS BY FREQUENCY\n')
        top20 = word_counts[0:20]
        longest = max(len(word) for word, count in top20)
        
        i = 1
        for word, count in top20:
            print('%s.\t%-*s: %5s' % (i, longest+1, word, count))
            i = i + 1
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time: {} seconds".format(elapsed_time))