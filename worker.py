import rpyc
import string
import collections
import threading
import os

class MapReduceService(rpyc.Service):
    def __init__(self):
        self.lock = threading.Lock()

    def exposed_map(self, text_chunk):
        """Map step: tokenize and count words in text chunk."""
        with self.lock:
            STOP_WORDS = set([
                'a','an','and','are','as','be','by','for','if','in',
                'is','it','of','or','py','rst','that','the','to','with',
            ])
            TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
            counts = collections.defaultdict(int)

            from rpyc.utils.classic import obtain
            text_chunk = obtain(text_chunk)
            
            for line in text_chunk.splitlines():
                if line.lstrip().startswith('..'):
                    continue
                line = line.translate(TR)
                for word in line.split():
                    word = word.lower()
                    if word.isalpha() and word not in STOP_WORDS:
                        counts[word] += 1

            print(f"[Worker] Mapped chunk: {len(counts)} unique words")
            return dict(counts)
    
    def exposed_reduce(self, grouped_items):
        """Reduce step: sum counts for a subset of words."""
        with self.lock:
            # getting value directly through Marshalling
            from rpyc.utils.classic import obtain
            grouped_items = obtain(grouped_items)
            
            # Using Counter for efficient aggregation
            reduced = collections.Counter()
            for word, values in grouped_items.items():
                reduced[word] = sum(values)
            
            print(f"[Worker] Reduced {len(reduced)} keys")
            return dict(reduced)


if __name__ == "__main__":
    import sys
    from rpyc.utils.server import ThreadedServer
    
    port = int(os.getenv("WORKER_PORT", sys.argv[1] if len(sys.argv) > 1 else 18861))
    t = ThreadedServer(
        MapReduceService,
        port=port,
        hostname="0.0.0.0",
        protocol_config={
            "allow_pickle": True,
            "allow_all_attrs": True,
            "allow_getattr": True,
            "sync_request_timeout": 300,
        },
    )
    print(f"[Worker] RPyC Worker running on port {port}...")
    t.start()
