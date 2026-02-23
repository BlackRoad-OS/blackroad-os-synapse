"""Event streaming and message bus (Matrix Synapse / event bus inspired)."""

import sqlite3
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional, Dict, Callable
import hashlib
import os


@dataclass
class Event:
    """Event in event bus."""
    id: str
    room_id: str
    type: str
    sender: str
    content: Dict = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    prev_events: List[str] = field(default_factory=list)
    auth_events: List[str] = field(default_factory=list)
    depth: int = 0
    sha256: str = ""
    processed: bool = False


@dataclass
class Room:
    """Chat room."""
    id: str
    alias: str
    creator: str
    join_rules: str = "public"  # public/invite/knock
    power_levels: Dict = field(default_factory=dict)
    state_events: List[str] = field(default_factory=list)
    members: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class Federation:
    """Federated server."""
    server_id: str
    server_name: str
    status: str = "connected"
    last_event_id: Optional[str] = None
    tls_cert_sha256: str = ""
    connected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class EventBus:
    """Event streaming and message bus."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize event bus with SQLite backend."""
        if db_path is None:
            db_path = os.path.expanduser("~/.blackroad/event_bus.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self.subscriptions: Dict[str, List[Callable]] = {}
        self._init_db()

    def _init_db(self):
        """Initialize database schema with FTS5."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                room_id TEXT NOT NULL,
                type TEXT NOT NULL,
                sender TEXT NOT NULL,
                content TEXT,
                timestamp TEXT,
                prev_events TEXT,
                auth_events TEXT,
                depth INTEGER,
                sha256 TEXT,
                processed BOOLEAN,
                FOREIGN KEY(room_id) REFERENCES rooms(id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                id TEXT PRIMARY KEY,
                alias TEXT UNIQUE,
                creator TEXT NOT NULL,
                join_rules TEXT,
                power_levels TEXT,
                state_events TEXT,
                members TEXT,
                created_at TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS federation (
                server_id TEXT PRIMARY KEY,
                server_name TEXT UNIQUE NOT NULL,
                status TEXT,
                last_event_id TEXT,
                tls_cert_sha256 TEXT,
                connected_at TEXT
            )
        """)

        # FTS5 for content search
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
                id,
                content,
                content='events',
                content_rowid='rowid'
            )
        """)

        conn.commit()
        conn.close()

    def create_room(self, creator: str, alias: str = "", join_rules: str = "public", topic: str = "") -> str:
        """Create a new room."""
        room_id = hashlib.sha256(f"{creator}{alias}{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]

        power_levels = {creator: 100}  # Creator is admin
        room = Room(id=room_id, alias=alias, creator=creator, join_rules=join_rules,
                   power_levels=power_levels, members=[creator])

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO rooms (id, alias, creator, join_rules, power_levels, state_events, members, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (room_id, alias, creator, join_rules, json.dumps(power_levels),
              json.dumps([]), json.dumps([creator]), room.created_at))

        # Create initial state event
        state_event_id = self._create_state_event(room_id, "m.room.create",
                                                   creator, {"topic": topic})

        conn.commit()
        conn.close()
        return room_id

    def put_event(self, room_id: str, event_type: str, sender: str, content: Dict) -> str:
        """Put event in room."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get previous events
        cursor.execute("SELECT id, depth FROM events WHERE room_id = ? ORDER BY depth DESC LIMIT 1",
                      (room_id,))
        prev_row = cursor.fetchone()
        prev_events = [prev_row[0]] if prev_row else []
        depth = (prev_row[1] if prev_row else 0) + 1

        # Calculate hash
        event_data = json.dumps({"type": event_type, "sender": sender, "content": content})
        sha256 = hashlib.sha256(event_data.encode()).hexdigest()

        # Create event
        event_id = hashlib.sha256(f"{room_id}{event_type}{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]

        event = Event(id=event_id, room_id=room_id, type=event_type, sender=sender,
                     content=content, prev_events=prev_events, depth=depth, sha256=sha256)

        cursor.execute("""
            INSERT INTO events (id, room_id, type, sender, content, timestamp, prev_events, depth, sha256, processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (event.id, room_id, event_type, sender, json.dumps(content),
              event.timestamp, json.dumps(prev_events), depth, sha256, False))

        # Index in FTS
        cursor.execute("INSERT INTO events_fts (id, content) VALUES (?, ?)",
                      (event_id, json.dumps(content)))

        conn.commit()
        conn.close()

        # Dispatch
        self.dispatch_event(event_id)

        return event_id

    def _create_state_event(self, room_id: str, event_type: str, sender: str, content: Dict) -> str:
        """Create a state event."""
        return self.put_event(room_id, event_type, sender, content)

    def get_events(self, room_id: str, from_depth: int = 0, limit: int = 100, reverse: bool = False) -> List[Event]:
        """Get events from room."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        order = "DESC" if reverse else "ASC"
        cursor.execute(f"""
            SELECT * FROM events WHERE room_id = ? AND depth >= ? ORDER BY depth {order} LIMIT ?
        """, (room_id, from_depth, limit))

        rows = cursor.fetchall()
        events = []

        for row in rows:
            event = Event(
                id=row[0], room_id=row[1], type=row[2], sender=row[3],
                content=json.loads(row[4]) if row[4] else {},
                timestamp=row[5], prev_events=json.loads(row[6]) if row[6] else [],
                auth_events=json.loads(row[7]) if row[7] else [],
                depth=row[8], sha256=row[9], processed=row[10]
            )
            events.append(event)

        conn.close()
        return events

    def get_state(self, room_id: str) -> Dict:
        """Get current room state."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM rooms WHERE id = ?", (room_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return {}

        room = Room(id=row[0], alias=row[1], creator=row[2], join_rules=row[3],
                   power_levels=json.loads(row[4]), state_events=json.loads(row[5]),
                   members=json.loads(row[6]), created_at=row[7])

        conn.close()

        return asdict(room)

    def join_room(self, room_id: str, user_id: str) -> bool:
        """Join room."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT members FROM rooms WHERE id = ?", (room_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return False

        members = json.loads(row[0])
        if user_id not in members:
            members.append(user_id)
            cursor.execute("UPDATE rooms SET members = ? WHERE id = ?",
                         (json.dumps(members), room_id))
            conn.commit()

            # Create join event
            self.put_event(room_id, "m.room.member", user_id, {"membership": "join"})

        conn.close()
        return True

    def leave_room(self, room_id: str, user_id: str) -> bool:
        """Leave room."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT members FROM rooms WHERE id = ?", (room_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return False

        members = json.loads(row[0])
        if user_id in members:
            members.remove(user_id)
            cursor.execute("UPDATE rooms SET members = ? WHERE id = ?",
                         (json.dumps(members), room_id))
            conn.commit()

            # Create leave event
            self.put_event(room_id, "m.room.member", user_id, {"membership": "leave"})

        conn.close()
        return True

    def set_power_level(self, room_id: str, user_id: str, level: int, by: str) -> bool:
        """Set user power level."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT power_levels FROM rooms WHERE id = ?", (room_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return False

        power_levels = json.loads(row[0])
        if power_levels.get(by, 0) >= 100:  # Only admin can change power levels
            power_levels[user_id] = level
            cursor.execute("UPDATE rooms SET power_levels = ? WHERE id = ?",
                         (json.dumps(power_levels), room_id))
            conn.commit()

        conn.close()
        return True

    def subscribe(self, event_type_pattern: str, callback_name: str):
        """Subscribe to event pattern."""
        if event_type_pattern not in self.subscriptions:
            self.subscriptions[event_type_pattern] = []
        self.subscriptions[event_type_pattern].append(callback_name)

    def dispatch_event(self, event_id: str):
        """Dispatch event to subscribers."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT type FROM events WHERE id = ?", (event_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return

        event_type = row[0]

        # Match subscriptions
        for pattern, callbacks in self.subscriptions.items():
            if self._match_pattern(pattern, event_type):
                for callback_name in callbacks:
                    # Mock: just call callback
                    pass

    def _match_pattern(self, pattern: str, event_type: str) -> bool:
        """Match event type against pattern."""
        if pattern == "*":
            return True
        if pattern == event_type:
            return True
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return event_type.startswith(prefix + ".")
        return False

    def get_federation_state(self) -> List[Federation]:
        """Get connected federated servers."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM federation WHERE status = 'connected'")
        rows = cursor.fetchall()

        federations = []
        for row in rows:
            fed = Federation(server_id=row[0], server_name=row[1], status=row[2],
                           last_event_id=row[3], tls_cert_sha256=row[4],
                           connected_at=row[5])
            federations.append(fed)

        conn.close()
        return federations

    def register_federation(self, server_name: str, server_id: str, tls_cert_sha256: str = "") -> bool:
        """Register federated server."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        fed = Federation(server_id=server_id, server_name=server_name,
                        tls_cert_sha256=tls_cert_sha256)

        try:
            cursor.execute("""
                INSERT INTO federation (server_id, server_name, status, tls_cert_sha256, connected_at)
                VALUES (?, ?, ?, ?, ?)
            """, (server_id, server_name, "connected", tls_cert_sha256, fed.connected_at))
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            conn.close()
            return False

    def backfill(self, room_id: str, from_event_id: str, limit: int = 20) -> List[Event]:
        """Get older events for backfill."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get depth of from_event_id
        cursor.execute("SELECT depth FROM events WHERE id = ?", (from_event_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return []

        from_depth = row[0]

        # Get events before that depth
        cursor.execute(f"""
            SELECT * FROM events WHERE room_id = ? AND depth < ? ORDER BY depth DESC LIMIT ?
        """, (room_id, from_depth, limit))

        rows = cursor.fetchall()
        events = []

        for row in rows:
            event = Event(
                id=row[0], room_id=row[1], type=row[2], sender=row[3],
                content=json.loads(row[4]) if row[4] else {},
                timestamp=row[5], prev_events=json.loads(row[6]) if row[6] else [],
                auth_events=json.loads(row[7]) if row[7] else [],
                depth=row[8], sha256=row[9], processed=row[10]
            )
            events.append(event)

        conn.close()
        return events


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Event Bus CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Rooms
    subparsers.add_parser("rooms")

    # Timeline
    timeline_parser = subparsers.add_parser("timeline")
    timeline_parser.add_argument("room_id")
    timeline_parser.add_argument("--limit", type=int, default=20)

    # Put event
    put_parser = subparsers.add_parser("put")
    put_parser.add_argument("room_id")
    put_parser.add_argument("event_type")
    put_parser.add_argument("sender")
    put_parser.add_argument("content")

    args = parser.parse_args()
    bus = EventBus()

    if args.command == "rooms":
        conn = sqlite3.connect(bus.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, alias, created_at FROM rooms")
        rooms = cursor.fetchall()
        conn.close()
        for room in rooms:
            print(f"{room[1]}: {room[0]} ({room[2]})")
    elif args.command == "timeline":
        events = bus.get_events(args.room_id, limit=args.limit)
        for event in events:
            print(f"[{event.depth}] {event.sender} ({event.type}): {event.content}")
    elif args.command == "put":
        content = json.loads(args.content)
        event_id = bus.put_event(args.room_id, args.event_type, args.sender, content)
        print(f"Event created: {event_id}")
