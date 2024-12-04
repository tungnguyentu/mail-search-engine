from typing import Dict
from whoosh.fields import Schema, TEXT, DATETIME, KEYWORD, ID, BOOLEAN
from whoosh.analysis import StemmingAnalyzer, CharsetFilter, RegexTokenizer
from whoosh.support.charset import accent_map
from whoosh.qparser import MultifieldParser, QueryParser
from whoosh.writing import AsyncWriter
import threading
from queue import Queue
import logging

class EmailIndexer:
    def __init__(self, ix):
        self.ix = ix
        self.write_queue = Queue()
        self.analyzer = self._create_analyzer()
        self._start_async_indexing()

    def _create_analyzer(self):
        """Create a custom analyzer with advanced features"""
        # Custom analyzer with accent handling and stemming
        analyzer = StemmingAnalyzer() | CharsetFilter(accent_map)
        return analyzer

    def _start_async_indexing(self):
        """Start asynchronous indexing thread"""
        self.indexing_thread = threading.Thread(
            target=self._process_index_queue,
            daemon=True
        )
        self.indexing_thread.start()

    def _process_index_queue(self):
        """Process queued indexing operations"""
        while True:
            try:
                operation = self.write_queue.get()
                if operation is None:
                    break

                action, data = operation
                writer = AsyncWriter(self.ix)
                
                try:
                    if action == 'add':
                        logging.info(f"Adding document to index: {data}")
                        writer.add_document(**data)
                    elif action == 'update':
                        logging.info(f"Updating document in index: {data}")
                        writer.update_document(**data)
                    elif action == 'delete':
                        logging.info(f"Deleting document from index: {data}")
                        writer.delete_by_term('email_id', data)
                    
                    writer.commit()
                    logging.info(f"Successfully committed {action} operation")
                except Exception as e:
                    writer.cancel()
                    logging.error(f"Indexing error: {e}", exc_info=True)
                
                self.write_queue.task_done()
            except Exception as e:
                logging.error(f"Queue processing error: {e}", exc_info=True)

    def add_document(self, document: Dict):
        """Queue document for addition"""
        self.write_queue.put(('add', document))

    def update_document(self, document: Dict):
        """Queue document for update"""
        self.write_queue.put(('update', document))

    def delete_document(self, email_id: str):
        """Queue document for deletion"""
        self.write_queue.put(('delete', email_id))

    def stop(self):
        """Stop the indexing thread"""
        self.write_queue.put(None)
        self.indexing_thread.join() 