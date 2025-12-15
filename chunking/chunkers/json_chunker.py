# chunking/chunkers/json_chunker.py

import json
import logging
from .base_chunker import BaseChunker
from dependencies import get_config

app_config_client = get_config()

class JSONChunker(BaseChunker):
    """
    JSONChunker is a custom chunker for well-formatted JSON files.
    It parses the JSON and recursively partitions the structure into smaller valid JSON chunks
    that do not exceed the maximum token limit.
    """

    def __init__(self, data, max_chunk_size=None, token_overlap=None, minimum_chunk_size=None):
        super().__init__(data)
        import os
        self.max_chunk_size = int(max_chunk_size or app_config_client.get("CHUNKING_NUM_TOKENS", 2048))
        self.token_overlap = int(token_overlap or app_config_client.get("TOKEN_OVERLAP", 100))
        self.minimum_chunk_size = int(minimum_chunk_size or app_config_client.get("CHUNKING_MIN_CHUNK_SIZE", 100))

    def get_chunks(self):
        """
        Splits the JSON content into chunks while ensuring each chunk is valid JSON.
        The method:
          1. Decodes document bytes to text.
          2. Parses the JSON.
          3. Uses a recursive partitioning algorithm to split the parsed JSON into valid pieces
             whose pretty-printed form is within the token limit.
          4. Creates chunk dictionaries from the resulting pieces.
        """
        if not self.document_bytes:
            logging.error(f"[json_chunker][{self.filename}] No document bytes provided.")
            return []

        text = self.decode_to_utf8(self.document_bytes)
        try:
            parsed_json = json.loads(text)
        except json.JSONDecodeError as e:
            logging.error(f"[json_chunker][{self.filename}] Error parsing JSON: {e}")
            return []

        # Recursively partition the parsed JSON
        partitioned = self._recursive_chunk_json(parsed_json)

        # Pretty-print each partition and filter by token count
        chunk_texts = []
        for part in partitioned:
            dumped = json.dumps(part, indent=2, ensure_ascii=False)
            token_count = self.token_estimator.estimate_tokens(dumped)
            if token_count >= self.minimum_chunk_size:
                chunk_texts.append(dumped)

        chunk_dicts = []
        chunk_id = 0
        for chunk_text in chunk_texts:
            token_count = self.token_estimator.estimate_tokens(chunk_text)
            if token_count > self.max_chunk_size:
                logging.warning(
                    f"[json_chunker][{self.filename}] A chunk still exceeds max tokens ({token_count} > {self.max_chunk_size})."
                    " This may happen if a single element is very large."
                )
                # Optionally, you might decide to leave such chunks as is,
                # or further process them with a string splitter.
            chunk_dict = self._create_chunk(chunk_id, chunk_text)
            chunk_dicts.append(chunk_dict)
            chunk_id += 1

        logging.info(f"[json_chunker][{self.filename}] Created {len(chunk_dicts)} chunk(s).")
        return chunk_dicts

    def _recursive_chunk_json(self, obj):
        """
        Recursively partition a JSON object (list or dict) so that each partition's
        pretty-printed string does not exceed self.max_chunk_size tokens.
        
        Returns a list of JSON-compatible Python objects.
        """
        def token_count_of(data):
            dumped = json.dumps(data, indent=2, ensure_ascii=False)
            return self.token_estimator.estimate_tokens(dumped)

        # If obj is a list, partition its items.
        if isinstance(obj, list):
            partitions = []
            current = []
            for item in obj:
                candidate = current + [item]
                if token_count_of(candidate) <= self.max_chunk_size:
                    current.append(item)
                else:
                    if current:
                        # Recursively check the current partition in case a single element is too large.
                        if token_count_of(current) > self.max_chunk_size and len(current) == 1:
                            partitions.extend(self._recursive_chunk_json(current[0]))
                        else:
                            partitions.append(current)
                    # If the item itself is too big, try to partition it further.
                    if token_count_of([item]) > self.max_chunk_size and isinstance(item, (list, dict)):
                        partitions.extend(self._recursive_chunk_json(item))
                    else:
                        current = [item]
            if current:
                partitions.append(current)
            return partitions

        # If obj is a dict, partition its key-value pairs.
        elif isinstance(obj, dict):
            partitions = []
            current = {}
            for key, value in obj.items():
                candidate = current.copy()
                candidate[key] = value
                if token_count_of(candidate) <= self.max_chunk_size:
                    current[key] = value
                else:
                    if current:
                        # If a single key-value pair is too large, try to partition its value.
                        if token_count_of(current) > self.max_chunk_size and len(current) == 1:
                            # current has one key; try partitioning its value if possible.
                            k = list(current.keys())[0]
                            v = current[k]
                            if isinstance(v, (list, dict)):
                                subparts = self._recursive_chunk_json(v)
                                for sub in subparts:
                                    partitions.append({k: sub})
                            else:
                                partitions.append(current)
                        else:
                            partitions.append(current)
                    # Try partitioning the new key-value pair if it's too large.
                    single = {key: value}
                    if token_count_of(single) > self.max_chunk_size and isinstance(value, (list, dict)):
                        subparts = self._recursive_chunk_json(value)
                        for sub in subparts:
                            partitions.append({key: sub})
                    else:
                        current = {key: value}
            if current:
                partitions.append(current)
            return partitions

        # For primitives, just return them as a single partition.
        else:
            return [obj]
