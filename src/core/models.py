from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, List
from datetime import datetime

class EventType(Enum):
    MESSAGE_NEW = "MessageNew"
    MESSAGE_FLAG = "MessageFlag"
    MESSAGE_DELETE = "MessageDelete"

@dataclass
class EmailEvent:
    event_type: EventType
    email_id: str
    timestamp: str
    data: Dict

@dataclass
class EmailAddress:
    name: str
    email: str

@dataclass
class EmailDocument:
    message_id: str
    email: str
    from_addr: List[EmailAddress]
    to_addr: List[EmailAddress]
    cc_addr: List[EmailAddress]
    bcc_addr: List[EmailAddress]
    subject: str
    body: str
    date: datetime
    labels: List[str]
    has_attachment: bool
    attachment_names: str
    is_read: bool
    is_starred: bool
    read_date: Optional[datetime] = None
