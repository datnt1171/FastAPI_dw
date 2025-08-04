# app/core/sql_loader.py
from typing import Dict

class SQLLoader:
    def __init__(self):
        self._queries: Dict[str, str] = {}
    
    def load_query(self, file_path: str) -> str:
        """Load SQL query from absolute file path with caching"""
        if file_path not in self._queries:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    self._queries[file_path] = f.read().strip()
            except FileNotFoundError:
                raise FileNotFoundError(f"SQL file not found: {file_path}")
        
        return self._queries[file_path]
    
    def reload_query(self, file_path: str) -> str:
        """Force reload query from file (useful in development)"""
        if file_path in self._queries:
            del self._queries[file_path]
        return self.load_query(file_path)

# Global instance
sql_loader = SQLLoader()