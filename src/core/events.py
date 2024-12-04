from datetime import datetime
from typing import Dict
from .models import EventType, EmailEvent

def create_new_message_event(email_data: Dict) -> Dict:
    return {
        "event_type": EventType.MESSAGE_NEW.value,
        "email_id": email_data["email_id"],
        "timestamp": datetime.now().isoformat(),
        "data": email_data
    }

def create_read_message_event(email_id: str) -> Dict:
    return {
        "event_type": EventType.MESSAGE_READ.value,
        "email_id": email_id,
        "timestamp": datetime.now().isoformat(),
        "data": {}
    }

def create_unread_message_event(email_id: str) -> Dict:
    return {
        "event_type": EventType.MESSAGE_UNREAD.value,
        "email_id": email_id,
        "timestamp": datetime.now().isoformat(),
        "data": {}
    }

def create_delete_message_event(email_id: str) -> Dict:
    return {
        "event_type": EventType.MESSAGE_DELETE.value,
        "email_id": email_id,
        "timestamp": datetime.now().isoformat(),
        "data": {}
    }
