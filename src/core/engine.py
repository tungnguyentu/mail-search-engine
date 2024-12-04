import os
import logging
from typing import Dict, List, Optional
from datetime import datetime
import threading
from whoosh import index
from whoosh.fields import Schema, TEXT, DATETIME, KEYWORD, ID, BOOLEAN
from whoosh.qparser import MultifieldParser, QueryParser
from whoosh.query import DateRange, Term, And, Or
from whoosh.analysis import StemmingAnalyzer
import json
from kafka import KafkaConsumer
import boto3
from slugify import slugify
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import re

from .models import EventType, EmailEvent
from src.config.settings import settings
from .cache import SearchCache
from .indexing import EmailIndexer


class EmailSearchEngine:
    def __init__(self, index_dir: str = settings.INDEX_DIR):
        """Initialize the search engine"""
        self.index_dir = index_dir
        self.analyzer = StemmingAnalyzer()
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            endpoint_url=settings.S3_ENDPOINT_URL
        )
        
        # Define schema with updated fields
        self.schema = Schema(
            email_id=ID(stored=True),
            message_id=ID(stored=True, unique=True),
            thread_id=ID(stored=True),
            email=ID(stored=True),
            from_addr=TEXT(stored=True, analyzer=self.analyzer),
            to_addr=TEXT(stored=True, analyzer=self.analyzer),
            cc_addr=TEXT(stored=True, analyzer=self.analyzer),
            bcc_addr=TEXT(stored=True, analyzer=self.analyzer),
            subject=TEXT(stored=True, analyzer=self.analyzer),
            body=TEXT(stored=True, analyzer=self.analyzer),
            date=DATETIME(stored=True),
            has_attachment=BOOLEAN(stored=True),
            attachment_names=KEYWORD(stored=True, commas=True),
            labels=KEYWORD(stored=True, commas=True, lowercase=True),
            is_read=BOOLEAN(stored=True),
            is_starred=BOOLEAN(stored=True),
            read_date=DATETIME(stored=True)
        )
        
        # Initialize cache and indexer
        self._create_or_recreate_index()
        self.cache = SearchCache()
        self.indexer = EmailIndexer(self.ix)

    def _create_or_recreate_index(self):
        """Create new index or recreate if schema changed"""
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)
            self.ix = index.create_in(self.index_dir, self.schema)
        else:
            # Check if we need to recreate the index
            try:
                self.ix = index.open_dir(self.index_dir)
                # Verify schema
                current_schema = self.ix.schema
                if 'labels' not in current_schema.names():
                    logging.info("Recreating index with updated schema")
                    import shutil
                    shutil.rmtree(self.index_dir)
                    os.makedirs(self.index_dir)
                    self.ix = index.create_in(self.index_dir, self.schema)
            except Exception as e:
                logging.error(f"Error opening index: {e}")
                logging.info("Creating new index")
                self.ix = index.create_in(self.index_dir, self.schema)

    def _start_event_consumer(self):
        """Start Kafka consumer in a separate thread"""
        self.consumer = KafkaConsumer(
            settings.EMAIL_EVENTS_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='email_search_engine'
        )
        consumer_thread = threading.Thread(
            target=self._consume_events,
            daemon=True
        )
        consumer_thread.start()

    def _consume_events(self):
        """Consume events from Kafka"""
        for message in self.consumer:
            try:
                event_data = message.value
                event = EmailEvent(
                    event_type=EventType(event_data['event_type']),
                    email_id=event_data['email_id'],
                    timestamp=event_data['timestamp'],
                    data=event_data['data']
                )
                print(event)
                self._process_event(event)
            except Exception as e:
                logging.error(f"Error processing event: {e}")

    def _process_event(self, event: EmailEvent):
        """Process incoming email events"""
        try:
            if event.event_type == EventType.MESSAGE_NEW:
                self._handle_new_message(event)
            elif event.event_type == EventType.MESSAGE_FLAG:
                self._handle_message_flag(event)
            elif event.event_type == EventType.MESSAGE_DELETE:
                self._handle_message_delete(event)
        except Exception as e:
            logging.error(f"Error processing event: {e}", exc_info=True)

    def _get_body_from_s3(self, email: str, message_id: str, label: str) -> str:
        """Fetch email body from S3 for specific label"""
        try:
            label = slugify(label)
            # Construct S3 key using email, label, and message_id
            s3_key = f"{email}/{label}/{message_id}.txt"
            
            # Get object from S3
            response = self.s3_client.get_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=s3_key
            )
            
            # Read body content
            body = response['Body'].read().decode('utf-8')
            logging.info(f"Successfully fetched body for message {message_id} from label {label}")
            return body
            
        except ClientError as e:
            logging.error(f"Error fetching body from S3 for message {message_id}, label {label}: {e}")
            return ""
        except Exception as e:
            logging.error(f"Unexpected error fetching body from S3: {e}")
            return ""

    def _handle_new_message(self, event: EmailEvent):
        """Handle new message event with async indexing"""
        try:
            email_data = event.data.copy()
            email_data['email_id'] = event.email_id
            email_data['message_id'] = email_data.get('message_id')
            email_data['thread_id'] = email_data.get('thread_id')

            # Get body for each label
            bodies = []
            for label in email_data.get('labels', ''):
                body = self._get_body_from_s3(
                    email=email_data.get('email'),
                    message_id=email_data.get('message_id'),
                    label=label
                )
                if body:
                    # Clean HTML content before adding to bodies
                    cleaned_body = self._clean_html_content(body)
                    if cleaned_body:
                        bodies.append(cleaned_body)
            
            # Combine bodies if multiple labels exist
            email_data['body'] = "\n---\n".join(bodies)

            # Process email addresses
            def format_addresses(addresses):
                if not addresses:
                    return ""
                if isinstance(addresses, str):
                    return addresses  # Already formatted
                formatted = []
                for addr in addresses:
                    name = addr.get('name', '').strip()
                    email = addr.get('email', '').strip()
                    if name and email:
                        formatted.append(f"{name} <{email}>")
                    elif email:
                        formatted.append(email)
                return " ; ".join(formatted)

            # Format addresses for indexing
            email_data['from_addr'] = format_addresses(email_data.get('from_addr', []))
            email_data['to_addr'] = format_addresses(email_data.get('to_addr', []))
            email_data['cc_addr'] = format_addresses(email_data.get('cc_addr', []))
            email_data['bcc_addr'] = format_addresses(email_data.get('bcc_addr', []))

            # Parse date
            if 'date' in email_data:
                try:
                    # Convert ISO format date string to datetime object
                    date_str = email_data['date']
                    if isinstance(date_str, str):
                        # Remove the 'Z' and handle the 'T' separator
                        date_str = date_str.replace('Z', '').replace('T', ' ')
                        email_data['date'] = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    logging.error(f"Error parsing date {email_data.get('date')}: {e}")
                    email_data['date'] = datetime.now()  # Use current time as fallback

            # Ensure has_attachment is boolean
            email_data['has_attachment'] = bool(email_data.get('has_attachment', False))
            
            # Format labels as comma-separated string if it's a list
            if isinstance(email_data.get('labels'), list):
                email_data['labels'] = ','.join(email_data['labels'])
            
            # Debug log
            logging.info(f"Indexing document: {email_data}")
            
            # Queue document for indexing
            self.indexer.add_document(email_data)
            
            # Cache the document
            self.cache.cache_document(email_data['message_id'], email_data)
            
            logging.info(f"Successfully queued message for indexing: {email_data['message_id']}")
        except Exception as e:
            logging.error(f"Error handling new message: {e}", exc_info=True)

    def _handle_message_flag(self, event: EmailEvent):
        """Handle message flag event"""
        try:
            email_data = event.data.copy()
            message_id = email_data.get('message_id')
            is_read = event.data.get('is_read')
            is_starred = event.data.get('is_starred')
            
            if not message_id:
                logging.error(f"No message_id provided in flag event: {event}")
                return

            logging.info(f"Processing flag event for message {message_id}")
            logging.info(f"Flags - is_read: {is_read}, is_starred: {is_starred}")
            
            # Search for the message
            with self.ix.searcher() as searcher:
                query = Term("message_id", message_id)
                results = searcher.search(query)
                
                if not results:
                    logging.error(f"Message {message_id} not found")
                    return
                
                # Update the single message
                email_data = dict(results[0])
                
                # Update read status if provided
                if is_read is not None:
                    email_data['is_read'] = is_read
                    if is_read:
                        email_data['read_date'] = datetime.now()
                    else:
                        email_data['read_date'] = None
                
                # Update starred status if provided
                if is_starred is not None:
                    email_data['is_starred'] = is_starred
                
                # Queue update operation
                self.indexer.update_document(email_data)
                
                # Update cache
                self.cache.invalidate_document(message_id)
                
                logging.info(f"Updated flags for message {message_id}")
            
        except Exception as e:
            logging.error(f"Error handling message flag event: {e}", exc_info=True)

    def _handle_message_delete(self, event: EmailEvent):
        """Handle message deletion with cache invalidation"""
        try:
            # Queue document for deletion
            self.indexer.delete_document(event.email_id)
            
            # Invalidate cache
            self.cache.invalidate_document(event.email_id)
            
            logging.info(f"Queued message deletion: {event.email_id}")
        except Exception as e:
            logging.error(f"Error handling message deletion: {e}")

    def search(self,
               email_id: str,
               subject: str = None,
               body: str = None,
               from_addr: str = None,
               to_addr: str = None,
               from_date: str = None,
               to_date: str = None,
               has_attachment: bool = None,
               labels: List[str] = None,
               page: int = 1,
               page_size: int = 10) -> Dict:
        """Enhanced search with caching"""
        try:
            # Return empty results if email_id is not provided
            if not email_id:
                return {
                    'total': 0,
                    'page': page,
                    'page_size': page_size,
                    'total_pages': 0,
                    'results': []
                }

            cache_key_params = {
                'email_id': email_id,
                'subject': subject,
                'body': body,
                'from_addr': from_addr,
                'to_addr': to_addr,
                'from_date': from_date,
                'to_date': to_date,
                'has_attachment': has_attachment,
                'labels': labels,
                'page': page,
                'page_size': page_size
            }
            
            cached_results = self.cache.get_search_results(**cache_key_params)
            if cached_results:
                logging.info("Cache hit for search query")
                return cached_results

            results = self._perform_search(**cache_key_params)
            self.cache.cache_search_results(cache_key_params, results)
            
            return results
        except Exception as e:
            logging.error(f"Search error: {e}")
            raise

    def _perform_search(self, **search_params) -> Dict:
        """Internal search implementation"""
        with self.ix.searcher() as searcher:
            try:
                queries = []
                
                # Email ID search (required)
                email_id = search_params.get('email_id')
                if not email_id:
                    return {
                        'total': 0,
                        'page': search_params.get('page', 1),
                        'page_size': search_params.get('page_size', 10),
                        'total_pages': 0,
                        'results': []
                    }
                queries.append(Term("email_id", email_id))

                # Address search (including CC/BCC for to_addr search)
                if search_params.get('to_addr'):
                    to_query = QueryParser("to_addr", self.schema).parse(search_params['to_addr'])
                    cc_query = QueryParser("cc_addr", self.schema).parse(search_params['to_addr'])
                    bcc_query = QueryParser("bcc_addr", self.schema).parse(search_params['to_addr'])
                    # Match if address appears in any recipient field
                    queries.append(Or([to_query, cc_query, bcc_query]))
                
                if search_params.get('from_addr'):
                    from_query = QueryParser("from_addr", self.schema).parse(search_params['from_addr'])
                    queries.append(from_query)

                # Add other search criteria
                if search_params.get('subject'):
                    subject_query = QueryParser("subject", self.schema).parse(search_params['subject'])
                    queries.append(subject_query)
                
                if search_params.get('body'):
                    body_query = QueryParser("body", self.schema).parse(search_params['body'])
                    queries.append(body_query)
                
                if search_params.get('from_date') or search_params.get('to_date'):
                    from_dt = datetime.strptime(search_params['from_date'], '%Y-%m-%d') if search_params.get('from_date') else None
                    to_dt = datetime.strptime(search_params['to_date'], '%Y-%m-%d') if search_params.get('to_date') else None
                    date_query = DateRange("date", from_dt, to_dt)
                    queries.append(date_query)
                
                if search_params.get('has_attachment') is not None:
                    has_attachment = (
                        search_params['has_attachment'] 
                        if isinstance(search_params['has_attachment'], bool)
                        else search_params['has_attachment'].lower() == 'true'
                    )
                    queries.append(Term("has_attachment", has_attachment))
                
                if search_params.get('labels'):
                    label_queries = []
                    for label in search_params['labels']:
                        label_queries.append(Term("labels", label.lower()))
                    if len(label_queries) > 1:
                        queries.append(Or(label_queries))
                    else:
                        queries.append(label_queries[0])

                # Combine all queries with AND
                final_query = And(queries)
                logging.info(f"Final query: {final_query}")

                # Execute search
                results = searcher.search_page(
                    final_query,
                    pagenum=search_params.get('page', 1),
                    pagelen=search_params.get('page_size', 10)
                )
                
                return {
                    'total': len(results),
                    'page': search_params.get('page', 1),
                    'page_size': search_params.get('page_size', 10),
                    'total_pages': (len(results) + search_params.get('page_size', 10) - 1) // search_params.get('page_size', 10),
                    'results': [self._format_result(r) for r in results]
                }
                
            except Exception as e:
                logging.error(f"Search execution error: {e}", exc_info=True)
                raise

    def _format_result(self, result) -> Dict:
        """Format search result"""
        def parse_addresses(addr_str):
            if not addr_str:
                return []
            addresses = []
            for addr in addr_str.split(';'):
                addr = addr.strip()
                if '<' in addr and '>' in addr:
                    name = addr[:addr.find('<')].strip()
                    email = addr[addr.find('<')+1:addr.find('>')].strip()
                    addresses.append({"name": name, "email": email})
                else:
                    addresses.append({"name": "", "email": addr})
            return addresses

        return {
            'email_id': result['email_id'],
            'thread_id': result['thread_id'],
            'from_addr': parse_addresses(result['from_addr']),
            'to_addr': parse_addresses(result['to_addr']),
            'cc_addr': parse_addresses(result['cc_addr']),
            'bcc_addr': parse_addresses(result['bcc_addr']),
            'subject': result['subject'],
            'date': result['date'],
            'snippet': result['body'][:200] + '...' if len(result['body']) > 200 else result['body'],
            'has_attachment': result['has_attachment'],
            'attachment_names': result.get('attachment_names').split(',') if result.get('attachment_names') else [],
            'labels': result['labels'].split(',') if result['labels'] else [],
            'is_read': result.get('is_read'),
            'is_starred': result.get('is_starred'),
            'read_date': result.get('read_date')
        }

    def cleanup(self):
        """Cleanup resources"""
        self.indexer.stop()
        self.cache.clear_all()

    def _clean_html_content(self, html_content: str) -> str:
        """Convert HTML to readable text"""
        try:
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text content
            text = soup.get_text()
            
            # Break into lines and remove leading/trailing space
            lines = (line.strip() for line in text.splitlines())
            
            # Break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            
            # Drop blank lines
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            # Remove extra whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text
        except Exception as e:
            logging.error(f"Error cleaning HTML content: {e}")
            return html_content  # Return original content if parsing fails
