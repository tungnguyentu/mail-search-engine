from functools import lru_cache
from typing import Dict, Optional
import hashlib
import json
from datetime import datetime, timedelta
from cachetools import TTLCache, LRUCache

class SearchCache:
    def __init__(self, maxsize=1000, ttl=3600):
        # TTL cache for search results (1 hour expiry)
        self.result_cache = TTLCache(maxsize=maxsize, ttl=ttl)
        # LRU cache for frequently accessed documents
        self.document_cache = LRUCache(maxsize=maxsize)

    def _generate_cache_key(self, **search_params) -> str:
        """Generate a unique cache key from search parameters"""
        # Sort parameters to ensure consistent key generation
        sorted_params = sorted(
            [(k, str(v)) for k, v in search_params.items() if v is not None]
        )
        key_string = json.dumps(sorted_params)
        return hashlib.md5(key_string.encode()).hexdigest()

    def get_search_results(self, **search_params) -> Optional[Dict]:
        """Get cached search results"""
        cache_key = self._generate_cache_key(**search_params)
        return self.result_cache.get(cache_key)

    def cache_search_results(self, search_params: Dict, results: Dict):
        """Cache search results"""
        cache_key = self._generate_cache_key(**search_params)
        self.result_cache[cache_key] = results

    def get_document(self, email_id: str) -> Optional[Dict]:
        """Get cached document"""
        return self.document_cache.get(email_id)

    def cache_document(self, email_id: str, document: Dict):
        """Cache document"""
        self.document_cache[email_id] = document

    def invalidate_document(self, email_id: str):
        """Invalidate document cache"""
        if email_id in self.document_cache:
            del self.document_cache[email_id]
        
    def clear_all(self):
        """Clear all caches"""
        self.result_cache.clear()
        self.document_cache.clear() 