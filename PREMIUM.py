# main.py - Video Merger Bot with Speed Booster Enhancements & Live Speed Display (MBps)
# 🔥 SPEED BOOST: Optimized thumbnail extraction
# ⚡ LIVE SPEED: Real-time download/upload speed in MBps
# ⏸️▶️ PAUSE/RESUME: Global and user-level pause with retry system + Inline Buttons
# 🌐 LANGUAGE SUPPORT: English / Hinglish (user selectable via /language)
# 🆕 DUPLICATE DETECTOR: Prevents re-uploading the same file (hash-based) with override option
# ❌ CANCEL BUTTON: User can cancel current job anytime during processing
# 🆕 METADATA SUPPORT: Add custom credits to videos (/metadata)
# 🟡 PREMIUM LOOK: Clean menu UI Systems added
# ❌ MERGE & ZIP FEATURES REMOVED - Only Single Video Upload Mode

import os
import asyncio
import logging
import re
import json
import subprocess
import time
import shutil
import sys
from datetime import datetime, timedelta
from collections import deque, defaultdict
from typing import Dict, Optional, List, Tuple, Any
from pathlib import Path
import sqlite3
import hashlib

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ChatAction, ParseMode
from pyrogram.errors import FloodWait, RPCError, MessageNotModified
from PIL import Image

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# ===== CONFIGURATION WITH VALIDATION =====
def get_env_var(key: str, default: Any = None, required: bool = True, var_type: type = str):
    """Get environment variable with validation"""
    value = os.getenv(key, default)
    
    if required and value is None:
        raise ValueError(f"❌ Missing required environment variable: {key}")
    
    if value is None:
        return None
    
    try:
        if var_type == int:
            return int(value)
        elif var_type == float:
            return float(value)
        elif var_type == bool:
            return value.lower() in ('true', '1', 'yes', 'on')
        else:
            return str(value)
    except ValueError as e:
        raise ValueError(f"❌ Invalid value for {key}: {value} - {e}")

# API Credentials - NO DEFAULTS for security!
API_ID = get_env_var("API_ID", var_type=int)
API_HASH = get_env_var("API_HASH")
BOT_TOKEN = get_env_var("BOT_TOKEN")

# Owner ID - Apna personal Telegram ID daalein
BOT_OWNER_ID = get_env_var("BOT_OWNER_ID", var_type=int)
BOT_USERNAME = get_env_var("BOT_USERNAME", "VideoUploaderBot", required=False)

# Channel configurations
MAIN_CHANNEL = get_env_var("MAIN_CHANNEL", "EntertainmentTadka786", required=False)
REQUEST_CHANNEL = get_env_var("REQUEST_CHANNEL", "EntertainmentTadka7860", required=False)
PRINTS_CHANNEL = get_env_var("PRINTS_CHANNEL", "threater_print_movies", required=False)
BACKUP_CHANNEL = get_env_var("BACKUP_CHANNEL", "ETBackup", required=False)
SERIAL_CHANNEL = get_env_var("SERIAL_CHANNEL", "Entertainment_Tadka_Serial_786", required=False)

# Format channel names with @
MAIN_CHANNEL = f"@{MAIN_CHANNEL}" if MAIN_CHANNEL and not MAIN_CHANNEL.startswith('@') else MAIN_CHANNEL
REQUEST_CHANNEL = f"@{REQUEST_CHANNEL}" if REQUEST_CHANNEL and not REQUEST_CHANNEL.startswith('@') else REQUEST_CHANNEL
PRINTS_CHANNEL = f"@{PRINTS_CHANNEL}" if PRINTS_CHANNEL and not PRINTS_CHANNEL.startswith('@') else PRINTS_CHANNEL
BACKUP_CHANNEL = f"@{BACKUP_CHANNEL}" if BACKUP_CHANNEL and not BACKUP_CHANNEL.startswith('@') else BACKUP_CHANNEL
SERIAL_CHANNEL = f"@{SERIAL_CHANNEL}" if SERIAL_CHANNEL and not SERIAL_CHANNEL.startswith('@') else SERIAL_CHANNEL

# File limits - Telegram bot actual limit is 2GB
MAX_FILE_SIZE = get_env_var("MAX_FILE_SIZE", 2000000000, var_type=int)  # 2GB in bytes
MAX_QUEUE_SIZE = get_env_var("MAX_QUEUE_SIZE", 100, var_type=int)
RATE_LIMIT_SECONDS = get_env_var("RATE_LIMIT_SECONDS", 1, var_type=int)  # Minimum time between user actions

# Directories
BASE_DIR = Path(__file__).parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
THUMB_DIR = BASE_DIR / "thumbs"
LOG_DIR = BASE_DIR / "logs"
OUTPUT_DIR = BASE_DIR / "output"
DB_DIR = BASE_DIR / "database"

# Create directories
for directory in [DOWNLOAD_DIR, THUMB_DIR, LOG_DIR, OUTPUT_DIR, DB_DIR]:
    directory.mkdir(exist_ok=True)

# ===== SETUP LOGGING WITH ROTATION =====
from logging.handlers import RotatingFileHandler

# Limit log file size to 10MB, keep 5 backup files
log_file = LOG_DIR / 'bot.log'
handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        handler,
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== DATABASE SETUP =====
class Database:
    """SQLite database for persistent storage"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path / "bot_database.sqlite"
        self.init_db()
    
    def init_db(self):
        """Initialize database tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Users table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        first_name TEXT,
                        username TEXT,
                        joined_date TIMESTAMP,
                        last_active TIMESTAMP,
                        total_files INTEGER DEFAULT 0,
                        total_size INTEGER DEFAULT 0
                    )
                ''')
                
                # Thumbnails table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS thumbnails (
                        user_id INTEGER PRIMARY KEY,
                        thumb_path TEXT,
                        thumb_hash TEXT,
                        created_at TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                ''')
                
                # Permanent names table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS permanent_names (
                        user_id INTEGER PRIMARY KEY,
                        template TEXT,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                ''')
                
                # Queue backup table (for crash recovery)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS queue_backup (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        message_data TEXT,
                        added_at TIMESTAMP
                    )
                ''')
                
                # Statistics table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date DATE,
                        total_uploads INTEGER DEFAULT 0,
                        total_size_gb REAL DEFAULT 0,
                        unique_users INTEGER DEFAULT 0
                    )
                ''')
                
                # [ADDED] Language preference table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_lang (
                        user_id INTEGER PRIMARY KEY,
                        lang_code TEXT DEFAULT 'en',
                        updated_at TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                ''')
                
                # [ADDED] File hashes table for duplicate detection
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS file_hashes (
                        hash TEXT PRIMARY KEY,
                        file_name TEXT,
                        file_size INTEGER,
                        user_id INTEGER,
                        uploaded_at TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                ''')
                
                # [ADDED] Metadata preferences table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_metadata (
                        user_id INTEGER PRIMARY KEY,
                        enabled INTEGER DEFAULT 0,
                        title_template TEXT,
                        artist_template TEXT,
                        updated_at TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                ''')
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise
    
    def execute(self, query: str, params: tuple = ()) -> Optional[List[tuple]]:
        """Execute a query with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    
                    if query.strip().upper().startswith('SELECT'):
                        result = cursor.fetchall()
                    else:
                        conn.commit()
                        result = None
                    
                    return result
                    
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                logger.error(f"Database error: {e}")
                raise
            except Exception as e:
                logger.error(f"Database error: {e}")
                raise
    
    def add_user(self, user_id: int, first_name: str, username: str = None):
        """Add or update user"""
        query = '''
            INSERT OR REPLACE INTO users 
            (user_id, first_name, username, joined_date, last_active) 
            VALUES (?, ?, ?, COALESCE((SELECT joined_date FROM users WHERE user_id=?), CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
        '''
        self.execute(query, (user_id, first_name, username, user_id))
    
    def update_user_activity(self, user_id: int):
        """Update user's last active timestamp"""
        query = 'UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?'
        self.execute(query, (user_id,))
    
    def save_thumbnail(self, user_id: int, thumb_path: str):
        """Save thumbnail reference with hash for deduplication"""
        try:
            # Calculate file hash for deduplication
            with open(thumb_path, 'rb') as f:
                thumb_hash = hashlib.md5(f.read()).hexdigest()
            
            # Check if same hash exists
            existing = self.execute(
                'SELECT thumb_path FROM thumbnails WHERE thumb_hash = ? AND user_id != ?',
                (thumb_hash, user_id)
            )
            
            if existing:
                # Same thumbnail exists for another user, we can reuse the file
                # But keep separate record per user
                logger.info(f"Thumbnail hash {thumb_hash[:8]} already exists in DB")
            
            query = '''
                INSERT OR REPLACE INTO thumbnails 
                (user_id, thumb_path, thumb_hash, created_at) 
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            '''
            self.execute(query, (user_id, str(thumb_path), thumb_hash))
            
        except Exception as e:
            logger.error(f"Error saving thumbnail to DB: {e}")
    
    def get_thumbnail(self, user_id: int) -> Optional[str]:
        """Get thumbnail path for user"""
        result = self.execute(
            'SELECT thumb_path FROM thumbnails WHERE user_id = ?',
            (user_id,)
        )
        return result[0][0] if result else None
    
    def save_permanent_name(self, user_id: int, template: str):
        """Save permanent name template"""
        query = '''
            INSERT OR REPLACE INTO permanent_names 
            (user_id, template, created_at, updated_at) 
            VALUES (?, ?, COALESCE((SELECT created_at FROM permanent_names WHERE user_id=?), CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
        '''
        self.execute(query, (user_id, template, user_id))
    
    def get_permanent_name(self, user_id: int) -> Optional[str]:
        """Get permanent name template for user"""
        result = self.execute(
            'SELECT template FROM permanent_names WHERE user_id = ?',
            (user_id,)
        )
        return result[0][0] if result else None
    
    def delete_permanent_name(self, user_id: int):
        """Delete permanent name template"""
        self.execute('DELETE FROM permanent_names WHERE user_id = ?', (user_id,))
    
    def delete_thumbnail(self, user_id: int):
        """Delete thumbnail reference"""
        self.execute('DELETE FROM thumbnails WHERE user_id = ?', (user_id,))
    
    def backup_queue(self, user_id: int, message_data: dict):
        """Backup queue item for crash recovery"""
        import json
        query = 'INSERT INTO queue_backup (user_id, message_data, added_at) VALUES (?, ?, CURRENT_TIMESTAMP)'
        self.execute(query, (user_id, json.dumps(message_data)))
    
    def clear_old_backups(self, hours: int = 24):
        """Clear backups older than specified hours"""
        query = 'DELETE FROM queue_backup WHERE added_at < datetime("now", "-? hours")'
        self.execute(query, (hours,))
    
    def update_stats(self, file_size: int, user_id: int):
        """Update statistics"""
        date_today = datetime.now().date()
        
        # Check if entry exists for today
        exists = self.execute(
            'SELECT id FROM stats WHERE date = ?',
            (date_today.isoformat(),)
        )
        
        if exists:
            query = '''
                UPDATE stats 
                SET total_uploads = total_uploads + 1,
                    total_size_gb = total_size_gb + ?,
                    unique_users = (SELECT COUNT(DISTINCT user_id) FROM (
                        SELECT user_id FROM users WHERE last_active >= date(?)
                    ))
                WHERE date = ?
            '''
            self.execute(query, (file_size / (1024**3), date_today.isoformat(), date_today.isoformat()))
        else:
            query = '''
                INSERT INTO stats (date, total_uploads, total_size_gb, unique_users)
                VALUES (?, 1, ?, 1)
            '''
            self.execute(query, (date_today.isoformat(), file_size / (1024**3)))
    
    # [ADDED] Language preference methods
    def set_user_lang(self, user_id: int, lang_code: str):
        """Set user's language preference"""
        query = '''
            INSERT OR REPLACE INTO user_lang (user_id, lang_code, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        '''
        self.execute(query, (user_id, lang_code))

    def get_user_lang(self, user_id: int) -> str:
        """Get user's language preference (default 'en')"""
        result = self.execute('SELECT lang_code FROM user_lang WHERE user_id = ?', (user_id,))
        return result[0][0] if result else 'en'

    # [ADDED] File hash methods for duplicate detection
    def insert_file_hash(self, file_hash: str, file_name: str, file_size: int, user_id: int) -> bool:
        """Insert file hash into database. Returns True if inserted, False if duplicate."""
        try:
            query = '''
                INSERT INTO file_hashes (hash, file_name, file_size, user_id, uploaded_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            '''
            self.execute(query, (file_hash, file_name, file_size, user_id))
            return True
        except sqlite3.IntegrityError:
            # Duplicate hash - already exists
            return False
        except Exception as e:
            logger.error(f"Error inserting file hash: {e}")
            return False  # Treat as duplicate to be safe

    # [ADDED] Metadata preference methods
    def set_metadata_enabled(self, user_id: int, enabled: bool):
        """Enable or disable metadata for user"""
        query = '''
            INSERT OR REPLACE INTO user_metadata 
            (user_id, enabled, updated_at) 
            VALUES (?, ?, CURRENT_TIMESTAMP)
        '''
        self.execute(query, (user_id, 1 if enabled else 0))
    
    def get_metadata_enabled(self, user_id: int) -> bool:
        """Get user's metadata preference (default False)"""
        result = self.execute(
            'SELECT enabled FROM user_metadata WHERE user_id = ?',
            (user_id,)
        )
        return bool(result[0][0]) if result else False

# Initialize database
db = Database(DB_DIR)

# ===== FFMPEG CHECK =====
def check_ffmpeg():
    """Check if FFmpeg is installed and working"""
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0]
            logger.info(f"FFmpeg found: {version_line}")
            return True
        else:
            logger.error("FFmpeg check failed")
            return False
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        logger.error(f"FFmpeg not found: {e}")
        return False

if not check_ffmpeg():
    logger.error("FFmpeg is required but not installed!")
    print("\n❌ FFMPEG NOT FOUND!")
    print("Please install FFmpeg:")
    print("  Ubuntu/Debian: sudo apt install ffmpeg")
    print("  CentOS/RHEL: sudo yum install ffmpeg")
    print("  Windows: Download from https://ffmpeg.org/download.html")
    sys.exit(1)

# ===== RATE LIMITER =====
class RateLimiter:
    """Rate limiting for user actions"""
    
    def __init__(self, limit_seconds: int = 1):
        self.limit_seconds = limit_seconds
        self.user_last_action: Dict[int, float] = {}
        self.user_action_count: Dict[int, List[float]] = defaultdict(list)
        self.max_actions_per_minute = 20
    
    def is_rate_limited(self, user_id: int) -> Tuple[bool, Optional[int]]:
        """Check if user is rate limited"""
        now = time.time()
        
        # Clean old actions
        self.user_action_count[user_id] = [
            t for t in self.user_action_count[user_id] 
            if now - t < 60
        ]
        
        # Check per-minute limit
        if len(self.user_action_count[user_id]) >= self.max_actions_per_minute:
            oldest = min(self.user_action_count[user_id])
            wait_time = int(60 - (now - oldest))
            return True, wait_time
        
        # Check per-action limit
        last_action = self.user_last_action.get(user_id, 0)
        if now - last_action < self.limit_seconds:
            return True, int(self.limit_seconds - (now - last_action))
        
        return False, None
    
    def record_action(self, user_id: int):
        """Record user action"""
        now = time.time()
        self.user_last_action[user_id] = now
        self.user_action_count[user_id].append(now)

rate_limiter = RateLimiter(RATE_LIMIT_SECONDS)

# ===== GLOBAL VARIABLES (With Database Backup) =====
# These are cached from database for performance
USER_THUMB: Dict[int, str] = {}  # Cache of thumbnail paths
USER_PERMANENT_NAMES: Dict[int, str] = {}  # Cache of permanent name templates
USER_LANG: Dict[int, str] = {}  # [ADDED] Cache of user language preference

# ===== PAUSE/RESUME & RETRY SYSTEM =====
QUEUE_PAUSED = False                # Global queue pause flag
QUEUE_PAUSE_EVENT = asyncio.Event()  # Event for queue pause
QUEUE_PAUSE_EVENT.set()              # Initially not paused

USER_PAUSED_JOBS: Dict[int, bool] = {}  # Track if user paused their current job
USER_PAUSE_EVENTS: Dict[int, asyncio.Event] = {}  # Events for user-specific pause

FAILED_JOBS: Dict[int, Dict] = {}    # Store failed job data for retry

# ===== CANCEL FLAG FOR CURRENT JOB =====
USER_CANCEL: Dict[int, bool] = {}  # True means user wants to cancel current job

# Load existing data from database
try:
    # Load thumbnails
    thumb_data = db.execute('SELECT user_id, thumb_path FROM thumbnails')
    if thumb_data:
        for user_id, thumb_path in thumb_data:
            if os.path.exists(thumb_path):
                USER_THUMB[user_id] = thumb_path
    
    # Load permanent names
    name_data = db.execute('SELECT user_id, template FROM permanent_names')
    if name_data:
        for user_id, template in name_data:
            USER_PERMANENT_NAMES[user_id] = template
    
    # [ADDED] Load user languages
    lang_data = db.execute('SELECT user_id, lang_code FROM user_lang')
    if lang_data:
        for uid, code in lang_data:
            USER_LANG[uid] = code
            
    logger.info(f"Loaded {len(USER_THUMB)} thumbnails, {len(USER_PERMANENT_NAMES)} permanent names, and {len(USER_LANG)} language preferences from DB")
except Exception as e:
    logger.error(f"Error loading from database: {e}")

# Other in-memory data (can be regenerated)
UPLOAD_QUEUE = deque(maxlen=MAX_QUEUE_SIZE)
IS_UPLOADING = False
USER_STATES: Dict[int, Dict] = {}
USER_CUSTOM_NAME: Dict[int, str] = {}  # Temporary custom name
USER_LAST_MESSAGE: Dict[int, float] = {}  # For duplicate prevention

# ===== BACKGROUND TASKS SET (to prevent garbage collection) =====
background_tasks = set()

# ===== PROGRESS TRACKING FOR SPEED =====
last_progress_info: Dict[int, Dict] = {}  # msg_id -> {'time': timestamp, 'bytes': current_bytes}

# ===== TYPING INDICATOR CONTEXT MANAGER =====
class TypingStatus:
    """Context manager for showing typing status with rate limiting"""
    
    _last_action: Dict[int, float] = {}
    
    @classmethod
    async def _send_action(cls, client: Client, chat_id: int, action: ChatAction):
        """Send chat action with rate limiting"""
        now = time.time()
        last = cls._last_action.get(chat_id, 0)
        
        # Don't send more than once every 4 seconds
        if now - last < 4:
            return
        
        try:
            await client.send_chat_action(chat_id, action)
            cls._last_action[chat_id] = now
        except Exception as e:
            logger.error(f"Failed to show typing: {e}")
    
    @classmethod
    async def show_typing(cls, client: Client, chat_id: int):
        await cls._send_action(client, chat_id, ChatAction.TYPING)
    
    @classmethod
    async def show_uploading_photo(cls, client: Client, chat_id: int):
        await cls._send_action(client, chat_id, ChatAction.UPLOAD_PHOTO)
    
    @classmethod
    async def show_uploading_video(cls, client: Client, chat_id: int):
        await cls._send_action(client, chat_id, ChatAction.UPLOAD_VIDEO)
    
    @classmethod
    async def show_uploading_document(cls, client: Client, chat_id: int):
        await cls._send_action(client, chat_id, ChatAction.UPLOAD_DOCUMENT)

# ===== PYROGRAM CLIENT =====
app = Client(
    name="video_uploader_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=100,
    sleep_threshold=10,
    max_concurrent_transmissions=20  # Limit concurrent uploads
)

# ===== UTILITY FUNCTIONS =====
def get_video_duration_ffprobe(video_path: str) -> int:
    """Get video duration using ffprobe"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_format', video_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            info = json.loads(result.stdout)
            duration = float(info['format']['duration'])
            return int(duration)
        return 0
    except Exception as e:
        logger.error(f"FFprobe error: {e}")
        return 0

def get_video_duration(message: Message, video_path: str = None) -> int:
    """Get video duration from message or file"""
    try:
        if message.video and message.video.duration:
            return message.video.duration
        
        if message.document and hasattr(message.document, 'duration'):
            return message.document.duration
        
        if video_path and os.path.exists(video_path):
            return get_video_duration_ffprobe(video_path)
        
        return 0
    except Exception as e:
        logger.error(f"Duration error: {e}")
        return 0

def clean_filename(filename: str) -> str:
    """Clean and format filename for display"""
    # Remove extension
    name_without_ext = os.path.splitext(filename)[0]
    
    # Remove common quality tags and metadata
    patterns_to_remove = [
        r'\b\d{3,4}p\b', r'\bhd\b', r'\bfhd\b', r'\buhd\b', r'\b4k\b',
        r'\bweb[\s.-]?dl\b', r'\bwebrip\b', r'\bbluray\b', r'\bhdrip\b',
        r'\bx264\b', r'\bx265\b', r'\bhevc\b', r'\baac\b', r'\besub\b',
        r'\bhin\b', r'\beng\b', r'\bhindi\b', r'\benglish\b', r'\bs\d+\b',
        r'\bep?\d+\b', r'\bseason\s\d+\b', r'\bepisode\s\d+\b',
        r'\[.*?\]', r'\(.*?\)',  # Remove anything in brackets
    ]
    
    cleaned = name_without_ext
    for pattern in patterns_to_remove:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Replace separators with spaces
    cleaned = re.sub(r'[\.\_\-]+', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # Capitalize words
    words = cleaned.split()
    cleaned = ' '.join(word.capitalize() for word in words if word)
    
    return cleaned if cleaned else name_without_ext[:50]

def format_file_size(size_bytes: int) -> str:
    """Format file size to readable format"""
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.2f} GB"
    elif size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} B"

def generate_bold_caption(filename: str) -> str:
    """Generate caption with all channels"""
    caption = f"**{filename}**\n\n"
    caption += "🔥 **Channels:**\n"
    
    channels = []
    if MAIN_CHANNEL:
        channels.append(f"🍿 **Main:** {MAIN_CHANNEL}")
    if REQUEST_CHANNEL:
        channels.append(f"📥 **Request:** {REQUEST_CHANNEL}")
    if PRINTS_CHANNEL:
        channels.append(f"🎭 **Theater:** {PRINTS_CHANNEL}")
    if BACKUP_CHANNEL:
        channels.append(f"📂 **Backup:** {BACKUP_CHANNEL}")
    if SERIAL_CHANNEL:
        channels.append(f"📺 **Serial:** {SERIAL_CHANNEL}")
    
    caption += '\n'.join(channels)
    return caption

def format_duration(seconds: int) -> str:
    """Format duration in HH:MM:SS"""
    if not seconds or seconds < 0:
        return "0:00"
    
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"

def is_safe_file_extension(filename: str) -> bool:
    """Check if file extension is safe"""
    safe_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.m4v', '.webm', '.flv', '.wmv', '.3gp'}
    ext = os.path.splitext(filename)[1].lower()
    return ext in safe_extensions

def get_unique_filename(directory: Path, filename: str) -> Path:
    """Get unique filename by adding counter if needed"""
    filepath = directory / filename
    
    if not filepath.exists():
        return filepath
    
    base = filepath.stem
    ext = filepath.suffix
    counter = 1
    
    while True:
        new_filename = f"{base}_{counter}{ext}"
        new_filepath = directory / new_filename
        if not new_filepath.exists():
            return new_filepath
        counter += 1

# ===== THUMBNAIL FUNCTIONS - NO QUALITY LOSS =====
def optimize_thumbnail_quality(thumb_path: Path):
    """
    Optimize thumbnail WITHOUT losing quality
    Just ensures it's in proper format for Telegram
    """
    try:
        img = Image.open(thumb_path)
        
        # Store original info
        original_size = img.size
        original_mode = img.mode
        
        # Convert to RGB if needed (Telegram works best with RGB)
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Save with MAXIMUM quality settings
        save_kwargs = {
            "format": "JPEG",
            "quality": 100,  # MAXIMUM QUALITY
            "subsampling": 0,  # No chroma subsampling
            "optimize": True,
            "progressive": True
        }
        
        img.save(thumb_path, **save_kwargs)
        
        logger.info(f"Thumbnail optimized: {original_size[0]}x{original_size[1]} -> RGB, 100% quality")
        
    except Exception as e:
        logger.error(f"Thumbnail optimization failed: {e}")
        # Don't raise - keep original file

def get_video_dimensions(message: Message) -> tuple:
    """Get video dimensions from message"""
    try:
        if message.video:
            width = message.video.width or 1280
            height = message.video.height or 720
            return (width, height)
        else:
            return (1280, 720)
    except:
        return (1280, 720)

def get_video_name(message: Message) -> str:
    """Extract video filename from message"""
    if message.video and message.video.file_name:
        return message.video.file_name
    elif message.document and message.document.file_name:
        return message.document.file_name
    else:
        return f"video_{message.id}.mp4"

def cleanup_files(*paths):
    """Cleanup files safely"""
    for path in paths:
        try:
            if path:
                path_obj = Path(path)
                if path_obj.exists():
                    if path_obj.is_file():
                        path_obj.unlink()
                        logger.info(f"Cleaned up: {path}")
                    elif path_obj.is_dir():
                        shutil.rmtree(path_obj)
                        logger.info(f"Cleaned up directory: {path}")
        except Exception as e:
            logger.error(f"Error cleaning {path}: {e}")

def create_progress_bar(percentage: float, width: int = 20) -> str:
    """Create visual progress bar"""
    filled = '█' * int(percentage / (100 / width))
    empty = '░' * (width - len(filled))
    return f"{filled}{empty}"

def format_time_remaining(seconds: int) -> str:
    """Format time remaining in human readable format"""
    if seconds < 60:
        return f"{seconds} seconds"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes > 1 else ''}"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours} hour{'s' if hours > 1 else ''} {minutes} minute{'s' if minutes > 1 else ''}"

# ===== [ADDED] LANGUAGE SUPPORT =====
LANGUAGES = {
    'en': 'English',
    'hi': 'Hinglish'
}

# Translation dictionary
LANG_TEXTS = {
    'start_welcome': {
        'en': (
            "👋 **Welcome {name}!** \n\n"
            "🤖 **I'm Video Uploader Bot** ✨\n\n"
            "🎬 **SINGLE VIDEO MODE ONLY:**\n"
            "1. Send thumbnail photo\n"
            "2. Send single video\n"
            "3. Auto-rename & upload\n\n"
            "⚡ **FEATURES:**\n"
            "✅ Thumbnail support (100% QUALITY)\n"
            "✅ Custom output names\n"
            "✅ Queue system\n"
            "✅ {max_file_size} file support\n"
            "✅ **Live typing indicators**\n"
            "✅ **⚡ LIVE SPEED: Real-time download/upload speed in MBps**\n"
            "✅ **⏸️ PAUSE/RESUME: Global and user-level pause**\n"
            "✅ **🔄 RETRY: Failed jobs can be retried**\n"
            "✅ **🆕 DUPLICATE DETECTION: Prevents re-uploading the same file (with override)**\n"
            "✅ **❌ CANCEL BUTTON: Cancel current job anytime**\n"
            "✅ **🎬 METADATA SUPPORT: Add custom credits to your videos (/metadata)**\n\n"
            "📢 **Channels:**\n"
        ),
        'hi': (
            "👋 **Namaste {name}!** \n\n"
            "🤖 **Main Video Uploader Bot hoon** ✨\n\n"
            "🎬 **SINGLE VIDEO MODE HI HAI:**\n"
            "1. Thumbnail photo bhejein\n"
            "2. Single video bhejein\n"
            "3. Auto-rename aur upload\n\n"
            "⚡ **FEATURES:**\n"
            "✅ Thumbnail support (100% QUALITY)\n"
            "✅ Custom output naam\n"
            "✅ Queue system\n"
            "✅ {max_file_size} file support\n"
            "✅ **Live typing indicators**\n"
            "✅ **⚡ LIVE SPEED: Real-time download/upload speed MBps mein**\n"
            "✅ **⏸️ PAUSE/RESUME: Global aur user-level pause**\n"
            "✅ **🔄 RETRY: Failed jobs ko retry kar sakte hain**\n"
            "✅ **🆕 DUPLICATE DETECTION: Ek hi file baar baar upload hone se rokta hai (override option ke saath)**\n"
            "✅ **❌ CANCEL BUTTON: Current job ko kabhi bhi cancel karein**\n"
            "✅ **🎬 METADATA SUPPORT: Apne videos mein credits add karein (/metadata)**\n\n"
            "📢 **Channels:**\n"
        )
    },
    'lang_set': {
        'en': "✅ Language set to English.",
        'hi': "✅ भाषा Hinglish कर दी गई है।"
    }
}

def get_text(user_id: int, key: str, **kwargs) -> str:
    """Return localized text for given user and key"""
    lang = USER_LANG.get(user_id, db.get_user_lang(user_id))
    if lang not in LANGUAGES:
        lang = 'en'
    text = LANG_TEXTS.get(key, {}).get(lang, LANG_TEXTS.get(key, {}).get('en', ''))
    if not text:
        logger.warning(f"Missing translation for key '{key}' in language '{lang}'")
        text = f"Missing translation: {key}"
    return text.format(**kwargs)

# ===== PREMIUM MENU UI =====
def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Generate premium main menu keyboard with categorized buttons"""
    keyboard = [
        [
            InlineKeyboardButton("🎬 Single Upload Guide", callback_data="mode_single"),
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="settings_menu"),
        ],
        [
            InlineKeyboardButton("🌐 Language", callback_data="language_menu"),
            InlineKeyboardButton("🎬 Metadata", callback_data="metadata_menu")
        ],
        [
            InlineKeyboardButton("📊 Status", callback_data="status"),
            InlineKeyboardButton("📋 Help", callback_data="help_menu")
        ],
        [
            InlineKeyboardButton("📢 Channels", callback_data="channels_menu"),
            InlineKeyboardButton("🔄 Refresh", callback_data="refresh_menu")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Settings submenu keyboard"""
    thumb_status = "✅ Set" if user_id in USER_THUMB else "❌ Not Set"
    perm_status = "✅ Set" if user_id in USER_PERMANENT_NAMES else "❌ Not Set"
    metadata_status = "✅ Enabled" if db.get_metadata_enabled(user_id) else "❌ Disabled"
    
    keyboard = [
        [
            InlineKeyboardButton(f"🖼️ Thumbnail {thumb_status}", callback_data="thumb_settings"),
        ],
        [
            InlineKeyboardButton(f"📛 Permanent Name {perm_status}", callback_data="permname_settings"),
        ],
        [
            InlineKeyboardButton(f"🎬 Metadata {metadata_status}", callback_data="metadata_menu"),
        ],
        [
            InlineKeyboardButton("🔙 Back", callback_data="main_menu")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_channels_keyboard() -> InlineKeyboardMarkup:
    """Channels keyboard with links"""
    buttons = []
    if MAIN_CHANNEL:
        buttons.append([InlineKeyboardButton("🍿 Main Channel", url=f"https://t.me/{MAIN_CHANNEL[1:]}")])
    if REQUEST_CHANNEL:
        buttons.append([InlineKeyboardButton("📥 Request Channel", url=f"https://t.me/{REQUEST_CHANNEL[1:]}")])
    if PRINTS_CHANNEL:
        buttons.append([InlineKeyboardButton("🎭 Theater Channel", url=f"https://t.me/{PRINTS_CHANNEL[1:]}")])
    if BACKUP_CHANNEL:
        buttons.append([InlineKeyboardButton("📂 Backup Channel", url=f"https://t.me/{BACKUP_CHANNEL[1:]}")])
    if SERIAL_CHANNEL:
        buttons.append([InlineKeyboardButton("📺 Serial Channel", url=f"https://t.me/{SERIAL_CHANNEL[1:]}")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
    return InlineKeyboardMarkup(buttons)

# ===== DOWNLOAD WITH RETRY AND PROGRESS =====
async def download_video_with_retry(
    message: Message, 
    output_path: Path, 
    status_msg: Message = None,
    max_retries: int = 3
) -> bool:
    """Download video with retry mechanism and progress tracking"""
    
    for attempt in range(max_retries):
        try:
            if status_msg:
                await status_msg.edit_text(
                    f"📥 **Downloading...** (Attempt {attempt + 1}/{max_retries})\n\n"
                    f"⏳ Please wait..."
                )
            
            # Download with progress
            file_path = await message.download(
                file_name=str(output_path),
                progress=download_progress,
                progress_args=(status_msg, f"Downloading (Attempt {attempt + 1})") if status_msg else None
            )
            
            if file_path and Path(file_path).exists() and Path(file_path).stat().st_size > 1024:
                return True
            else:
                logger.warning(f"Download attempt {attempt+1} created empty file: {output_path}")
                if output_path.exists():
                    output_path.unlink()
                
        except asyncio.TimeoutError:
            logger.warning(f"Download attempt {attempt+1} timed out")
            if output_path.exists():
                output_path.unlink()
        except Exception as e:
            logger.warning(f"Download attempt {attempt+1} failed: {e}")
            if output_path.exists():
                output_path.unlink()
        
        # Exponential backoff
        await asyncio.sleep(2 ** attempt)
    
    return False

# ===== PROGRESS CALLBACKS WITH RATE LIMITING AND LIVE SPEED (MBps) =====
last_progress_update: Dict[int, float] = {}

async def download_progress(current, total, status_msg: Message, operation: str):
    """Download progress callback with rate limiting and live speed (MBps)"""
    if not status_msg:
        return
    
    try:
        msg_id = status_msg.id
        now = time.time()
        
        # Check if user paused this job
        user_id = status_msg.chat.id
        if USER_PAUSED_JOBS.get(user_id, False):
            if user_id in USER_PAUSE_EVENTS:
                await USER_PAUSE_EVENTS[user_id].wait()
        
        # Check if user cancelled this job
        if USER_CANCEL.get(user_id, False):
            USER_CANCEL[user_id] = False
            logger.info(f"User {user_id} cancelled download")
            raise asyncio.CancelledError("User cancelled download")
        
        # Rate limit updates (every 2 seconds)
        if msg_id in last_progress_update and now - last_progress_update[msg_id] < 2:
            return
        last_progress_update[msg_id] = now
        
        if total > 0:
            percentage = (current * 100) / total
            downloaded = current / (1024 * 1024)
            total_mb = total / (1024 * 1024)
            
            # Calculate speed
            speed_str = "Calculating..."
            if msg_id in last_progress_info:
                last_time, last_bytes = last_progress_info[msg_id]['time'], last_progress_info[msg_id]['bytes']
                time_diff = now - last_time
                if time_diff > 0:
                    bytes_diff = current - last_bytes
                    speed_bps = bytes_diff / time_diff
                    speed_mbps = speed_bps / (1024 * 1024)
                    speed_str = f"{speed_mbps:.2f} MB/s"
            
            # Store current info
            last_progress_info[msg_id] = {'time': now, 'bytes': current}
            
            progress_bar = create_progress_bar(percentage)
            
            # Build keyboard with pause/resume buttons
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("⏸️ Pause", callback_data=f"pause_{user_id}"),
                InlineKeyboardButton("▶️ Resume", callback_data=f"resume_{user_id}")
            ]])
            
            await status_msg.edit_text(
                f"📥 **{operation}...**\n\n"
                f"{progress_bar} {percentage:.1f}%\n"
                f"📊 {downloaded:.1f}MB / {total_mb:.1f}MB\n"
                f"⚡ Speed: {speed_str}",
                reply_markup=keyboard
            )
            
            await TypingStatus.show_uploading_document(status_msg._client, status_msg.chat.id)
    except MessageNotModified:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"Download progress error: {e}")

async def upload_progress(current, total, status_msg: Message, operation: str):
    """Upload progress callback with rate limiting and live speed (MBps)"""
    if not status_msg:
        return
    
    try:
        msg_id = status_msg.id
        now = time.time()
        
        # Check if user paused this job
        user_id = status_msg.chat.id
        if USER_PAUSED_JOBS.get(user_id, False):
            if user_id in USER_PAUSE_EVENTS:
                await USER_PAUSE_EVENTS[user_id].wait()
        
        # Check if user cancelled this job
        if USER_CANCEL.get(user_id, False):
            USER_CANCEL[user_id] = False
            logger.info(f"User {user_id} cancelled upload")
            raise asyncio.CancelledError("User cancelled upload")
        
        # Rate limit updates (every 2 seconds)
        if msg_id in last_progress_update and now - last_progress_update[msg_id] < 2:
            return
        last_progress_update[msg_id] = now
        
        if total > 0:
            percentage = (current * 100) / total
            uploaded = current / (1024 * 1024)
            total_mb = total / (1024 * 1024)
            
            # Calculate speed
            speed_str = "Calculating..."
            if msg_id in last_progress_info:
                last_time, last_bytes = last_progress_info[msg_id]['time'], last_progress_info[msg_id]['bytes']
                time_diff = now - last_time
                if time_diff > 0:
                    bytes_diff = current - last_bytes
                    speed_bps = bytes_diff / time_diff
                    speed_mbps = speed_bps / (1024 * 1024)
                    speed_str = f"{speed_mbps:.2f} MB/s"
            
            # Store current info
            last_progress_info[msg_id] = {'time': now, 'bytes': current}
            
            progress_bar = create_progress_bar(percentage)
            
            # Build keyboard with pause/resume buttons
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("⏸️ Pause", callback_data=f"pause_{user_id}"),
                InlineKeyboardButton("▶️ Resume", callback_data=f"resume_{user_id}")
            ]])
            
            await status_msg.edit_text(
                f"📤 **{operation}...**\n\n"
                f"{progress_bar} {percentage:.1f}%\n"
                f"📊 {uploaded:.1f}MB / {total_mb:.1f}MB\n"
                f"⚡ Speed: {speed_str}",
                reply_markup=keyboard
            )
            
            if "video" in operation.lower():
                await TypingStatus.show_uploading_video(status_msg._client, status_msg.chat.id)
            else:
                await TypingStatus.show_uploading_document(status_msg._client, status_msg.chat.id)
    except MessageNotModified:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"Upload progress error: {e}")

# ===== CONTINUOUS ACTION FOR LONG PROCESSES =====
async def show_continuous_action(client: Client, chat_id: int, action_type: str, duration: int = 30):
    """Show continuous action (typing/uploading) for long processes"""
    try:
        start_time = time.time()
        last_action = 0
        
        while time.time() - start_time < duration:
            now = time.time()
            if now - last_action >= 5:
                try:
                    if action_type == "typing":
                        await TypingStatus.show_typing(client, chat_id)
                    elif action_type == "upload_video":
                        await TypingStatus.show_uploading_video(client, chat_id)
                    elif action_type == "upload_document":
                        await TypingStatus.show_uploading_document(client, chat_id)
                    last_action = now
                except Exception as e:
                    logger.error(f"Continuous action error: {e}")
                    break
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.debug(f"Continuous action cancelled for chat {chat_id}")
        raise

# ===== METADATA FUNCTION =====
def add_metadata_to_video(input_path: Path, user_id: int = None) -> Path:
    """
    Add custom metadata to video file.
    Returns path to the new file (or original if failed).
    """
    try:
        username = "@MNA_3786"
        channel = "@EntertainmentTadka786"
        
        output_path = input_path.parent / f"{input_path.stem}_meta{input_path.suffix}"
        
        cmd = [
            'ffmpeg',
            '-i', str(input_path),
            '-metadata', f'title=Encoded By :- {username}',
            '-metadata', f'author={username}',
            '-metadata', f'description=Subtitled By :- {channel}',
            '-metadata', f'comment=Audio: By :- {channel}\\nVideo: Encoded By :- {channel}',
            '-metadata', f'artist={username}',
            '-c:v', 'copy',
            '-c:a', 'copy',
            str(output_path),
            '-y'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0 and output_path.exists():
            logger.info(f"Metadata added to {input_path.name} -> {output_path.name}")
            input_path.unlink()
            return output_path
        else:
            logger.error(f"Metadata addition failed: {result.stderr[:200]}")
            return input_path
            
    except Exception as e:
        logger.error(f"Metadata error: {e}")
        return input_path

# ===== ADMIN COMMANDS =====
@app.on_message(filters.command("broadcast") & filters.user(BOT_OWNER_ID))
async def broadcast_command(client: Client, message: Message):
    """Broadcast message to all users (admin only)"""
    if len(message.command) < 2:
        await message.reply_text("❌ Please provide a message to broadcast!")
        return
    
    broadcast_text = message.text.split(' ', 1)[1]
    
    users = db.execute('SELECT user_id FROM users')
    if not users:
        await message.reply_text("❌ No users found in database!")
        return
    
    status_msg = await message.reply_text(f"🔄 Broadcasting to {len(users)} users...")
    
    success = 0
    failed = 0
    
    for (user_id,) in users:
        try:
            await client.send_message(
                user_id,
                f"📢 **Broadcast Message**\n\n{broadcast_text}"
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Broadcast failed for {user_id}: {e}")
    
    await status_msg.edit_text(
        f"✅ **Broadcast Complete!**\n\n"
        f"📊 **Stats:**\n"
        f"• Total users: {len(users)}\n"
        f"• Success: {success}\n"
        f"• Failed: {failed}"
    )

@app.on_message(filters.command("stats") & filters.user(BOT_OWNER_ID))
async def admin_stats_command(client: Client, message: Message):
    """Show detailed stats (admin only)"""
    today = datetime.now().date().isoformat()
    today_stats = db.execute(
        'SELECT total_uploads, total_size_gb, unique_users FROM stats WHERE date = ?',
        (today,)
    )
    
    total_users = db.execute('SELECT COUNT(*) FROM users')
    total_users = total_users[0][0] if total_users else 0
    
    active_today = db.execute(
        'SELECT COUNT(*) FROM users WHERE last_active >= date("now", "-1 day")'
    )
    active_today = active_today[0][0] if active_today else 0
    
    total_uploads = db.execute('SELECT SUM(total_uploads) FROM stats')
    total_single = total_uploads[0][0] if total_uploads and total_uploads[0] else 0
    
    download_size = sum(f.stat().st_size for f in DOWNLOAD_DIR.glob('**/*') if f.is_file())
    thumb_size = sum(f.stat().st_size for f in THUMB_DIR.glob('**/*') if f.is_file())
    
    stats_text = f"""
📊 **Admin Statistics**

👥 **Users:**
• Total users: {total_users}
• Active today: {active_today}
• Active now: {len(USER_THUMB)}

📈 **Today's Stats:**
• Uploads: {today_stats[0][0] if today_stats else 0}
• Data uploaded: {today_stats[0][1] if today_stats else 0:.2f} GB
• Unique users: {today_stats[0][2] if today_stats else 0}

📊 **Total Stats:**
• Total uploads: {total_single}

💾 **Storage:**
• Downloads: {format_file_size(download_size)}
• Thumbnails: {format_file_size(thumb_size)}
• Queue size: {len(UPLOAD_QUEUE)}

⚙️ **System:**
• FFmpeg: ✅ Installed
• Database: ✅ Connected
• Rate limit: {RATE_LIMIT_SECONDS}s
• Max file size: {format_file_size(MAX_FILE_SIZE)}
• Queue paused: {'✅' if QUEUE_PAUSED else '❌'}
"""
    await message.reply_text(stats_text)

@app.on_message(filters.command("pausequeue") & filters.user(BOT_OWNER_ID))
async def pause_queue_command(client: Client, message: Message):
    """Pause the entire queue processing (admin only)"""
    global QUEUE_PAUSED, QUEUE_PAUSE_EVENT
    if QUEUE_PAUSED:
        await message.reply_text("⚠️ Queue is already paused.")
        return
    QUEUE_PAUSED = True
    QUEUE_PAUSE_EVENT.clear()
    await message.reply_text("⏸️ **Queue paused.**")
    logger.info("Queue paused by admin")

@app.on_message(filters.command("resumequeue") & filters.user(BOT_OWNER_ID))
async def resume_queue_command(client: Client, message: Message):
    """Resume the entire queue processing (admin only)"""
    global QUEUE_PAUSED, QUEUE_PAUSE_EVENT
    if not QUEUE_PAUSED:
        await message.reply_text("⚠️ Queue is already running.")
        return
    QUEUE_PAUSED = False
    QUEUE_PAUSE_EVENT.set()
    await message.reply_text("▶️ **Queue resumed.**")
    logger.info("Queue resumed by admin")

# ===== COMMAND HANDLERS =====
@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    """Handle /start command with premium menu"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    
    now = time.time()
    if user.id in USER_LAST_MESSAGE and now - USER_LAST_MESSAGE[user.id] < 2:
        return
    USER_LAST_MESSAGE[user.id] = now
    
    await TypingStatus.show_typing(client, message.chat.id)
    
    db.add_user(user.id, user.first_name, user.username)
    
    welcome_text = get_text(
        user.id, 
        'start_welcome', 
        name=user.first_name,
        max_file_size=format_file_size(MAX_FILE_SIZE)
    )
    
    welcome_text += """
**Commands:**
/start - Main menu
/filerename - Set output name
/thumb - Set thumbnail
/myformat - Check settings
/status - Check queue
/pause - Pause your current job
/resume - Resume your paused job
/cancel - Cancel operation
/help - Show help
/stats - Bot statistics
/language - Choose your language
/metadata - Add credits to videos
/menu - Show main menu
"""
    
    await message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(user.id))
    logger.info(f"User {user.id} started the bot")

@app.on_message(filters.command("menu"))
async def menu_command(client: Client, message: Message):
    """Show the main menu"""
    user = message.from_user
    rate_limiter.record_action(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    await message.reply_text("📌 **Main Menu**", reply_markup=get_main_menu_keyboard(user.id))

@app.on_message(filters.command("filerename"))
async def filerename_command(client: Client, message: Message):
    """Custom rename command with permanent option"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    user_id = user.id
    
    if len(message.command) < 2:
        response = "📝 **Filename Renamer Help**\n\n"
        
        if user_id in USER_PERMANENT_NAMES:
            response += f"✅ **Current Permanent Template:**\n`{USER_PERMANENT_NAMES[user_id]}`\n\n"
        
        if user_id in USER_CUSTOM_NAME:
            response += f"📌 **Current Temporary Name:**\n`{USER_CUSTOM_NAME[user_id]}`\n\n"
        
        response += "**Usage:**\n"
        response += "**Temporary (current file only):**\n"
        response += "`/filerename Movie Name 2025`\n\n"
        response += "**Permanent (all future files):**\n"
        response += "`/filerename permanent My Template`\n\n"
        response += "**Reset permanent name:**\n"
        response += "`/filerename permanent reset`\n\n"
        response += "**Examples:**\n"
        response += "• `/filerename Sikandar 2025 Hindi`\n"
        response += "• `/filerename permanent My Movie`\n"
        response += "• `/filerename permanent reset`"
        
        await message.reply_text(response)
        return
    
    if message.command[1].lower() == "permanent":
        if len(message.command) < 3:
            await message.reply_text("❌ Please provide a template name!\n\nExample: `/filerename permanent My Movie`")
            return
        
        if message.command[2].lower() == "reset":
            USER_PERMANENT_NAMES.pop(user_id, None)
            db.delete_permanent_name(user_id)
            await message.reply_text("✅ **Permanent name reset successfully!**")
            return
        
        permanent_name = ' '.join(message.command[2:]).strip()
        
        if len(permanent_name) > 100:
            await message.reply_text("❌ Template name too long! Max 100 characters.")
            return
        
        USER_PERMANENT_NAMES[user_id] = permanent_name
        db.save_permanent_name(user_id, permanent_name)
        
        await message.reply_text(
            f"✅ **Permanent name template set!**\n\n"
            f"📝 **Template:** `{permanent_name}`\n\n"
            f"**Files will be automatically named:**\n"
            f"• `{permanent_name} 720p.mp4` (for HD files)\n"
            f"• `{permanent_name} 1080p.mp4` (for FHD files)\n"
            f"• `{permanent_name} 4K.mp4` (for 4K files)\n"
            f"• `{permanent_name}.mp4` (for others)"
        )
        return
    
    new_name = ' '.join(message.command[1:]).strip()
    
    if not any(new_name.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi', '.mov']):
        new_name += '.mp4'
    
    if not is_safe_file_extension(new_name):
        await message.reply_text("❌ **Unsafe file extension!**\n\nAllowed extensions: .mp4, .mkv, .avi, .mov, .m4v, .webm, .flv, .wmv, .3gp")
        return
    
    USER_CUSTOM_NAME[user_id] = new_name
    
    response = f"✅ **Temporary name set!**\n\n📁 **Current file:** `{new_name}`\n\n"
    if user_id in USER_PERMANENT_NAMES:
        response += f"📌 **Permanent template:** `{USER_PERMANENT_NAMES[user_id]}`\n(This temporary name will override permanent for current file only)\n\n"
    else:
        response += f"**Tip:** Use `/filerename permanent` to set a permanent template!\n\n"
    
    response += f"**Next:** Send your video file."
    
    await message.reply_text(response)
    logger.info(f"User {user_id} set temporary filename: {new_name}")

@app.on_message(filters.command("myformat"))
async def myformat_command(client: Client, message: Message):
    """Check your current filename format settings"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    user_id = user.id
    
    response = "📝 **Your Filename Settings**\n\n"
    
    if user_id in USER_PERMANENT_NAMES:
        template = USER_PERMANENT_NAMES[user_id]
        response += f"✅ **Permanent Template:**\n`{template}`\n\n"
        response += "**Future files format:**\n"
        response += f"• `{template} 720p.mp4` (if HD detected)\n"
        response += f"• `{template} 1080p.mp4` (if FHD detected)\n"
        response += f"• `{template} 4K.mp4` (if 4K detected)\n"
        response += f"• `{template}.mp4` (default)\n\n"
    else:
        db_template = db.get_permanent_name(user_id)
        if db_template:
            USER_PERMANENT_NAMES[user_id] = db_template
            response += f"✅ **Permanent Template (from DB):**\n`{db_template}`\n\n"
        else:
            response += "❌ **No permanent template set**\n\n"
    
    if user_id in USER_CUSTOM_NAME:
        response += f"📌 **Current temporary name:**\n`{USER_CUSTOM_NAME[user_id]}`\n\n"
    
    response += "**Commands:**\n"
    response += "• `/filerename permanent Template` - Set permanent\n"
    response += "• `/filerename permanent reset` - Reset permanent\n"
    response += "• `/filerename Temp Name` - Temporary rename\n"
    response += "• `/status` - Check queue"
    
    await message.reply_text(response)

@app.on_message(filters.command("thumb"))
async def thumb_command(client: Client, message: Message):
    """Handle /thumb command"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    user_id = user.id
    
    if user_id in USER_THUMB:
        thumb_path = Path(USER_THUMB[user_id])
        if thumb_path.exists():
            size = format_file_size(thumb_path.stat().st_size)
            await message.reply_text(
                f"⚠️ **You already have a thumbnail set!**\n\n"
                f"📏 Size: {size}\n"
                f"📍 Path: `{thumb_path.name}`\n\n"
                f"**Options:**\n"
                f"• Send new photo to replace\n"
                f"• Continue with current thumbnail\n"
                f"• Use `/clearthumb` to remove"
            )
            return
    
    await message.reply_text(
        "📷 **Please send a photo to set as thumbnail.**\n\n"
        "**Features:**\n"
        "• **100% QUALITY PRESERVED** - No blurring!\n"
        "• Original colors maintained\n"
        "• Auto-optimized for Telegram\n"
        "• Works with any image size\n"
        "• Saved to database permanently\n\n"
        "📤 **Send your thumbnail now...**"
    )
    USER_STATES[user.id] = {"awaiting_thumb": True}

@app.on_message(filters.command("clearthumb"))
async def clearthumb_command(client: Client, message: Message):
    """Clear user's thumbnail"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    user_id = user.id
    
    if user_id in USER_THUMB:
        thumb_path = USER_THUMB[user_id]
        if os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except:
                pass
        
        del USER_THUMB[user_id]
        db.delete_thumbnail(user_id)
        
        await message.reply_text("✅ **Thumbnail cleared successfully!**")
        logger.info(f"User {user_id} cleared thumbnail")
    else:
        await message.reply_text("❌ **No thumbnail set!**")

@app.on_message(filters.command(["status", "queue"]))
async def status_command(client: Client, message: Message):
    """Check queue status"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    queue_size = len(UPLOAD_QUEUE)
    active = "🟢 **Active**" if IS_UPLOADING else "🔴 **Idle**"
    user_id = user.id
    
    user_position = None
    user_job = None
    for i, (msg, data) in enumerate(UPLOAD_QUEUE, 1):
        if msg.from_user.id == user_id:
            user_position = i
            user_job = data
            break
    
    status_text = f"""
📊 **Bot Status Dashboard**

{active}
📥 **Queue Size:** {queue_size}/{MAX_QUEUE_SIZE} job(s)
⏳ **Estimated Wait:** {format_time_remaining(queue_size * 120)}
⏸️ **Queue Paused:** {'✅' if QUEUE_PAUSED else '❌'}

👤 **Your Status:**
• Thumbnail: {'✅ Set' if user_id in USER_THUMB else '❌ Not Set'}
• Temp Name: {'✅ Set' if user_id in USER_CUSTOM_NAME else '❌ Not Set'}
• Permanent Format: {'✅ Set' if user_id in USER_PERMANENT_NAMES else '❌ Not Set'}
• Job Paused: {'✅' if USER_PAUSED_JOBS.get(user_id, False) else '❌'}
• Metadata: {'✅ Enabled' if db.get_metadata_enabled(user_id) else '❌ Disabled'}
"""
    
    if user_position:
        status_text += f"\n📌 **Your Position:** #{user_position}"
    
    if queue_size > 0:
        status_text += f"\n\n📋 **Next {min(3, queue_size)} in queue:**\n"
        for i, (msg, data) in enumerate(list(UPLOAD_QUEUE)[:3], 1):
            filename = data.get("original_filename", "Unknown")
            status_text += f"{i}. 🎬 `{filename[:25]}...`\n"
    
    await message.reply_text(status_text)

@app.on_message(filters.command("pause"))
async def pause_job_command(client: Client, message: Message):
    """Pause user's current job"""
    user_id = message.from_user.id
    if user_id not in USER_PAUSE_EVENTS:
        USER_PAUSE_EVENTS[user_id] = asyncio.Event()
        USER_PAUSE_EVENTS[user_id].set()
    
    USER_PAUSED_JOBS[user_id] = True
    USER_PAUSE_EVENTS[user_id].clear()
    
    await message.reply_text("⏸️ **Your current job paused.** Use /resume to continue.")
    logger.info(f"User {user_id} paused their job")

@app.on_message(filters.command("resume"))
async def resume_job_command(client: Client, message: Message):
    """Resume user's paused job"""
    user_id = message.from_user.id
    if user_id in USER_PAUSE_EVENTS:
        USER_PAUSED_JOBS[user_id] = False
        USER_PAUSE_EVENTS[user_id].set()
        await message.reply_text("▶️ **Your job resumed.**")
        logger.info(f"User {user_id} resumed their job")
    else:
        await message.reply_text("❌ You don't have any paused job.")

@app.on_message(filters.command("cancel"))
async def cancel_command(client: Client, message: Message):
    """Cancel user's upload"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    user_id = user.id
    
    original_len = len(UPLOAD_QUEUE)
    new_queue = deque(maxlen=MAX_QUEUE_SIZE)
    for msg, data in UPLOAD_QUEUE:
        if msg.from_user.id != user_id:
            new_queue.append((msg, data))
    
    UPLOAD_QUEUE.clear()
    UPLOAD_QUEUE.extend(new_queue)
    
    removed = original_len - len(UPLOAD_QUEUE)
    
    cleanup_list = []
    
    if user_id in USER_STATES:
        USER_STATES.pop(user_id, None)
    
    try:
        for file in DOWNLOAD_DIR.glob(f"temp_{user_id}_*"):
            cleanup_list.append(file)
        for file in OUTPUT_DIR.glob(f"*"):
            if file.stat().st_mtime > time.time() - 3600:
                cleanup_list.append(file)
    except Exception as e:
        logger.error(f"Cancel cleanup error: {e}")
    
    USER_CUSTOM_NAME.pop(user_id, None)
    
    if cleanup_list:
        cleanup_files(*cleanup_list)
    
    response = f"✅ **Cancellation Complete!**\n\n• Removed {removed} job(s) from queue\n• Cleared temporary files\n\n"
    
    if user_id in USER_PERMANENT_NAMES:
        response += f"📌 **Permanent format preserved:** `{USER_PERMANENT_NAMES[user_id]}`\n\n"
    
    if user_id in USER_THUMB:
        response += f"🖼️ **Thumbnail preserved** (use `/clearthumb` to remove)\n\n"
    
    response += f"🔄 **Ready for new upload!**"
    
    await message.reply_text(response)
    logger.info(f"User {user_id} cancelled operations (removed {removed} jobs)")

@app.on_message(filters.command("help"))
async def help_command(client: Client, message: Message):
    """Handle /help command"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    help_text = f"""
🛠 **Video Uploader Bot - Help Guide**

🎬 **SINGLE VIDEO MODE:**
1. `/thumb` or send photo → Set thumbnail
2. Send video file → Add to queue
3. `/filerename` → Customize filename
4. Bot auto-processes → Uploads with caption

⚡ **FEATURES:**
• ✅ Thumbnail 100% quality
• ✅ Custom filename
• ✅ Queue system
• ✅ **Live speed display (MBps)**
• ✅ **Pause/Resume** with buttons
• ✅ **Retry failed jobs**
• ✅ **Duplicate detection** with override
• ✅ **Cancel button** during processing
• ✅ **Metadata** via /metadata

📋 **COMMANDS:**
• `/filerename` - Customize filename
• `/myformat` - Check settings
• `/thumb` - Set thumbnail
• `/clearthumb` - Remove thumbnail
• `/status` - Check queue
• `/pause` - Pause your current job
• `/resume` - Resume paused job
• `/cancel` - Cancel operation
• `/start` - Main menu
• `/help` - This help
• `/stats` - Bot statistics
• `/language` - Choose language
• `/metadata` - Toggle metadata
• `/menu` - Show main menu

⚠️ **LIMITS:**
• Max file size: {format_file_size(MAX_FILE_SIZE)}
• Supported formats: MP4, MKV, AVI, MOV

📢 **CHANNELS:**"""
    
    if MAIN_CHANNEL:
        help_text += f"\n{MAIN_CHANNEL}"
    if REQUEST_CHANNEL:
        help_text += f"\n{REQUEST_CHANNEL}"
    if PRINTS_CHANNEL:
        help_text += f"\n{PRINTS_CHANNEL}"
    if BACKUP_CHANNEL:
        help_text += f"\n{BACKUP_CHANNEL}"
    if SERIAL_CHANNEL:
        help_text += f"\n{SERIAL_CHANNEL}"
    
    await message.reply_text(help_text, reply_markup=get_main_menu_keyboard(user.id))

@app.on_message(filters.command("stats"))
async def stats_command(client: Client, message: Message):
    """Show bot statistics (public version)"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    try:
        total_users = db.execute('SELECT COUNT(*) FROM users')
        total_users = total_users[0][0] if total_users else 0
        
        active_today = db.execute(
            'SELECT COUNT(*) FROM users WHERE last_active >= date("now", "-1 day")'
        )
        active_today = active_today[0][0] if active_today else 0
        
        total_uploads = db.execute('SELECT SUM(total_uploads) FROM stats')
        total_single = total_uploads[0][0] if total_uploads and total_uploads[0] else 0
        
        stats_text = f"""
📊 **Bot Statistics**

👥 **Users:**
• Total users: {total_users}
• Active today: {active_today}

📈 **Operations:**
• Total uploads: {total_single}

⚙️ **System:**
• Queue size: {len(UPLOAD_QUEUE)}/{MAX_QUEUE_SIZE}
• Max file size: {format_file_size(MAX_FILE_SIZE)}
• Rate limit: {RATE_LIMIT_SECONDS}s
• **Live speed: ✅ Enabled (MBps)**
• **Queue paused: {'✅' if QUEUE_PAUSED else '❌'}**
• **Duplicate detection: ✅ Enabled**
• **Cancel button: ✅ Enabled**
• **Metadata: ✅ Available**

📢 **Channels:**"""
        
        if MAIN_CHANNEL:
            stats_text += f"\n• {MAIN_CHANNEL}"
        if REQUEST_CHANNEL:
            stats_text += f"\n• {REQUEST_CHANNEL}"
        if PRINTS_CHANNEL:
            stats_text += f"\n• {PRINTS_CHANNEL}"
        if BACKUP_CHANNEL:
            stats_text += f"\n• {BACKUP_CHANNEL}"
        if SERIAL_CHANNEL:
            stats_text += f"\n• {SERIAL_CHANNEL}"
        
        await message.reply_text(stats_text)
        
    except Exception as e:
        await message.reply_text(f"❌ Error getting stats: {str(e)}")
        logger.error(f"Stats error: {e}")

# ===== LANGUAGE COMMAND =====
@app.on_message(filters.command("language"))
async def language_command(client: Client, message: Message):
    """Let user choose their preferred language"""
    user = message.from_user
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
         InlineKeyboardButton("🇮🇳 Hinglish", callback_data="lang_hi")],
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
    ])
    await message.reply_text("Please select your language / भाषा चुनें:", reply_markup=keyboard)

# ===== METADATA COMMAND =====
@app.on_message(filters.command("metadata"))
async def metadata_command(client: Client, message: Message):
    """Toggle metadata addition"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    user_id = user.id
    current_status = db.get_metadata_enabled(user_id)
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ ON" if current_status else "⚪ ON", callback_data="meta_on"),
            InlineKeyboardButton("❌ OFF" if not current_status else "⚪ OFF", callback_data="meta_off")
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
    ])
    
    status_text = "**Metadata Settings**\n\n"
    status_text += f"Current: {'✅ Enabled' if current_status else '❌ Disabled'}\n\n"
    status_text += "When enabled, bot adds:\n"
    status_text += "• Title: Encoded By :- @MNA_3786\n"
    status_text += "• Author: @MNA_3786\n"
    status_text += "• Description: Subtitled By :- @EntertainmentTadka786\n"
    status_text += "• Comment: Audio/Video credits\n\n"
    status_text += "Choose option:"
    
    await message.reply_text(status_text, reply_markup=keyboard)

# ===== MEDIA HANDLERS =====
@app.on_message(filters.photo)
async def handle_photo(client: Client, message: Message):
    """Handle photo (thumbnail) - WITH QUALITY PRESERVATION"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_uploading_photo(client, message.chat.id)
    
    user_id = user.id
    
    try:
        thumb_path = THUMB_DIR / f"{user_id}_{int(time.time())}.jpg"
        
        await message.download(file_name=str(thumb_path))
        
        optimize_thumbnail_quality(thumb_path)
        
        img = Image.open(thumb_path)
        width, height = img.size
        file_size = thumb_path.stat().st_size
        
        if user_id in USER_THUMB:
            old_thumb = Path(USER_THUMB[user_id])
            if old_thumb.exists() and old_thumb != thumb_path:
                try:
                    old_thumb.unlink()
                except:
                    pass
        
        USER_THUMB[user_id] = str(thumb_path)
        db.save_thumbnail(user_id, str(thumb_path))
        
        if user_id in USER_STATES:
            USER_STATES[user_id].pop("awaiting_thumb", None)
            if not USER_STATES[user_id]:
                USER_STATES.pop(user_id, None)
        
        channel_text = f"\n\n📢 **Join:** {SERIAL_CHANNEL}" if SERIAL_CHANNEL else ""
        
        await message.reply_text(
            f"✅ **Thumbnail saved successfully!**\n\n"
            f"📏 **Dimensions:** {width}x{height}\n"
            f"💾 **Size:** {format_file_size(file_size)}\n"
            f"🎨 **Format:** JPEG (100% Quality){channel_text}\n\n"
            f"📤 **Now send me a video file (max {format_file_size(MAX_FILE_SIZE)})**"
        )
        logger.info(f"Thumbnail saved for user {user_id}: {width}x{height}, {format_file_size(file_size)}")
        
    except Exception as e:
        await message.reply_text(f"❌ Error saving thumbnail: {str(e)}")
        logger.error(f"Thumbnail error for user {user_id}: {e}")

# ===== HASH COMPUTATION FUNCTION =====
async def compute_file_hash(file_path: Path, chunk_size: int = 8192) -> str:
    """Compute SHA256 hash of a file asynchronously"""
    loop = asyncio.get_event_loop()
    def _hash():
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    return await loop.run_in_executor(None, _hash)

# ===== VIDEO HANDLER =====
@app.on_message(filters.video | filters.document)
async def handle_video(client: Client, message: Message):
    """Handle video files"""
    user = message.from_user
    
    is_limited, wait_time = rate_limiter.is_rate_limited(user.id)
    if is_limited:
        await message.reply_text(f"⏳ **Slow down!** Please wait {wait_time} seconds.")
        return
    
    rate_limiter.record_action(user.id)
    db.update_user_activity(user.id)
    await TypingStatus.show_typing(client, message.chat.id)
    
    user_id = user.id
    
    # Check if user has thumbnail
    if user_id not in USER_THUMB:
        db_thumb = db.get_thumbnail(user_id)
        if db_thumb and Path(db_thumb).exists():
            USER_THUMB[user_id] = db_thumb
        else:
            await message.reply_text(
                "❌ **Please set a thumbnail first!**\n\n"
                "**Two options:**\n"
                "1. Send any photo (auto-detected)\n"
                "2. Use `/thumb` command\n\n"
                f"Then send your video file again."
            )
            return
    
    try:
        if message.video:
            file_size = message.video.file_size
            mime_type = message.video.mime_type or "video/mp4"
            original_filename = message.video.file_name or f"video_{message.id}.mp4"
        elif message.document:
            file_size = message.document.file_size
            mime_type = message.document.mime_type or "application/octet-stream"
            original_filename = message.document.file_name or f"file_{message.id}.mp4"
        else:
            await message.reply_text("❌ Unsupported file type!")
            return
        
        if file_size > MAX_FILE_SIZE:
            await message.reply_text(
                f"❌ **File too large!**\n\n"
                f"Max size: {format_file_size(MAX_FILE_SIZE)}\n"
                f"Your file: {format_file_size(file_size)}\n\n"
                f"Please send a smaller file."
            )
            return
        
        if not is_safe_file_extension(original_filename):
            await message.reply_text("❌ **Unsafe file type!**\n\nPlease send video files only.\nAllowed: MP4, MKV, AVI, MOV, etc.")
            return
        
        if len(UPLOAD_QUEUE) >= MAX_QUEUE_SIZE:
            await message.reply_text(f"❌ **Queue is full!**\n\nPlease try again later.")
            return
        
        video_dimensions = get_video_dimensions(message)
        video_duration = get_video_duration(message)
        
        display_name = original_filename
        if user_id in USER_PERMANENT_NAMES:
            template = USER_PERMANENT_NAMES[user_id]
            if '720p' in original_filename.lower() or 'hd' in original_filename.lower() or video_dimensions[1] >= 720:
                display_name = f"{template} 720p.mp4"
            elif '1080p' in original_filename.lower() or 'fhd' in original_filename.lower() or video_dimensions[1] >= 1080:
                display_name = f"{template} 1080p.mp4"
            elif '4k' in original_filename.lower() or video_dimensions[1] >= 2160:
                display_name = f"{template} 4K.mp4"
            else:
                display_name = f"{template}.mp4"
        elif user_id in USER_CUSTOM_NAME:
            display_name = USER_CUSTOM_NAME[user_id]
        
        queue_data = {
            "user_id": user_id,
            "original_filename": original_filename,
            "file_size": file_size,
            "video_dimensions": video_dimensions,
            "video_duration": video_duration,
            "mime_type": mime_type,
            "display_name": display_name,
            "operation": "single",
            "chat_id": message.chat.id,
            "message_id": message.id,
            "timestamp": time.time()
        }
        
        try:
            db.backup_queue(user_id, queue_data)
        except Exception as e:
            logger.error(f"Failed to backup queue: {e}")
        
        UPLOAD_QUEUE.append((message, queue_data))
        
        position = len(UPLOAD_QUEUE)
        estimated_wait = position * 2
        
        channel_text = f"\n📢 **Join:** {SERIAL_CHANNEL}" if SERIAL_CHANNEL else ""
        
        await message.reply_text(
            f"📥 **Added to processing queue!**\n\n"
            f"📊 **Position:** #{position}\n"
            f"⏳ **Estimated wait:** ~{estimated_wait} minutes\n"
            f"📁 **File:** `{display_name[:40]}...`\n"
            f"📦 **Size:** {format_file_size(file_size)}{channel_text}\n\n"
            f"**Commands:**\n"
            f"• `/status` - Check progress\n"
            f"• `/filerename` - Customize name\n"
            f"• `/pause` - Pause this job\n"
            f"• `/resume` - Resume if paused\n"
            f"• `/cancel` - Remove from queue"
        )
        
        logger.info(f"Video '{original_filename}' added to queue by user {user_id}, position {position}")
        
        if not IS_UPLOADING:
            asyncio.create_task(process_queue(client))
            
    except Exception as e:
        await message.reply_text(f"❌ Error processing file: {str(e)}")
        logger.error(f"Video handle error for user {user_id}: {e}")

# ===== QUEUE PROCESSOR =====
async def process_queue(client: Client):
    """Process upload queue sequentially"""
    global IS_UPLOADING
    
    while UPLOAD_QUEUE:
        if QUEUE_PAUSED:
            logger.info("Queue paused, waiting...")
            await QUEUE_PAUSE_EVENT.wait()
        
        queue_item = UPLOAD_QUEUE[0]
        message = queue_item[0]
        data = queue_item[1]
        
        user_id = data["user_id"]
        
        if USER_PAUSED_JOBS.get(user_id, False):
            logger.info(f"User {user_id} paused their job, moving to end of queue")
            UPLOAD_QUEUE.rotate(-1)
            await asyncio.sleep(2)
            continue
        
        UPLOAD_QUEUE.popleft()
        
        logger.info(f"Processing job for user {user_id}")
        
        try:
            await process_single_video(client, message, data)
        except Exception as e:
            logger.error(f"Error processing job for user {user_id}: {e}", exc_info=True)
            try:
                await client.send_message(
                    data.get("chat_id", message.chat.id),
                    f"❌ **Processing Error**\n\n{str(e)[:200]}"
                )
            except:
                pass
        
        await asyncio.sleep(2)
    
    IS_UPLOADING = False
    logger.info("Queue processing completed")

# ===== PROCESS SINGLE VIDEO =====
async def process_single_video(client: Client, message: Message, data: dict):
    """Process single video with all features"""
    user_id = data["user_id"]
    original_filename = data["original_filename"]
    file_size = data["file_size"]
    video_dimensions = data["video_dimensions"]
    video_duration = data["video_duration"]
    display_name = data.get("display_name", original_filename)
    chat_id = data.get("chat_id", message.chat.id)
    
    status_msg = None
    temp_path = None
    new_path = None
    
    try:
        thumb_path = USER_THUMB.get(user_id)
        if not thumb_path or not Path(thumb_path).exists():
            db_thumb = db.get_thumbnail(user_id)
            if db_thumb and Path(db_thumb).exists():
                thumb_path = db_thumb
                USER_THUMB[user_id] = db_thumb
            else:
                await client.send_message(chat_id, "❌ Thumbnail not found! Please set again with /thumb")
                return
        
        await TypingStatus.show_typing(client, chat_id)
        
        status_msg = await client.send_message(
            chat_id,
            f"🔄 **Processing your video...**\n\n"
            f"📁 **File:** `{original_filename[:40]}...`\n"
            f"📊 **Size:** {format_file_size(file_size)}\n"
            f"⏱️ **Duration:** {format_duration(video_duration)}\n"
            f"📥 **Downloading...** (0%)"
        )
        
        control_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⏸️ Pause", callback_data=f"pause_{user_id}"),
                InlineKeyboardButton("▶️ Resume", callback_data=f"resume_{user_id}")
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{user_id}")]
        ])
        await status_msg.edit_text(status_msg.text, reply_markup=control_keyboard)
        
        force_duplicate = data.get("force_duplicate", False)
        
        if data.get("temp_path"):
            temp_path = Path(data["temp_path"])
            if not temp_path.exists():
                temp_path = None
        
        if not temp_path:
            if not message:
                raise ValueError("No message and no temp_path provided")
            
            task = asyncio.create_task(show_continuous_action(client, chat_id, "download", 300))
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
            
            temp_path = DOWNLOAD_DIR / f"temp_{user_id}_{int(time.time())}.mp4"
            download_success = await download_video_with_retry(message, temp_path, status_msg, max_retries=3)
            
            if not download_success or not temp_path.exists():
                FAILED_JOBS[user_id] = {
                    "type": "single",
                    "original_filename": original_filename,
                    "file_size": file_size,
                    "video_dimensions": video_dimensions,
                    "video_duration": video_duration,
                    "mime_type": data.get("mime_type"),
                    "display_name": display_name,
                    "chat_id": chat_id,
                    "message_id": message.id,
                    "error": "Download failed"
                }
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Retry", callback_data=f"retry_{user_id}_single")
                ]])
                await status_msg.edit_text("❌ Download failed!\nClick Retry to try again.", reply_markup=keyboard)
                return
            
            if not force_duplicate:
                await status_msg.edit_text("🔄 Checking for duplicates...", reply_markup=control_keyboard)
                file_hash = await compute_file_hash(temp_path)
                inserted = db.insert_file_hash(file_hash, original_filename, file_size, user_id)
                
                if not inserted:
                    logger.info(f"Duplicate file detected for user {user_id}: {original_filename}")
                    FAILED_JOBS[f"dup_{user_id}"] = {
                        "type": "single",
                        "original_filename": original_filename,
                        "file_size": file_size,
                        "video_dimensions": video_dimensions,
                        "video_duration": video_duration,
                        "mime_type": data.get("mime_type"),
                        "display_name": display_name,
                        "chat_id": chat_id,
                        "message_id": message.id,
                        "temp_path": str(temp_path),
                        "file_hash": file_hash,
                        "error": "Duplicate file"
                    }
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("⚠️ Upload Anyway", callback_data=f"dup_override_{user_id}")
                    ]])
                    await status_msg.edit_text(
                        f"⚠️ **Duplicate File Detected!**\n\n`{original_filename}` already exists.\nPress button to upload anyway.",
                        reply_markup=keyboard
                    )
                    return
        
        actual_duration = get_video_duration_ffprobe(str(temp_path))
        if actual_duration > 0:
            video_duration = actual_duration
        
        file_ext = Path(original_filename).suffix or ".mp4"
        
        if user_id in USER_PERMANENT_NAMES:
            template = USER_PERMANENT_NAMES[user_id]
            if '720p' in original_filename.lower() or video_dimensions[1] >= 720:
                new_filename = f"{template} 720p{file_ext}"
            elif '1080p' in original_filename.lower() or video_dimensions[1] >= 1080:
                new_filename = f"{template} 1080p{file_ext}"
            elif '4k' in original_filename.lower() or video_dimensions[1] >= 2160:
                new_filename = f"{template} 4K{file_ext}"
            else:
                new_filename = f"{template}{file_ext}"
        elif user_id in USER_CUSTOM_NAME:
            new_filename = USER_CUSTOM_NAME[user_id]
            if Path(new_filename).suffix != file_ext:
                new_filename = Path(new_filename).stem + file_ext
        else:
            name_without_ext = Path(original_filename).stem
            cleaned_name = clean_filename(name_without_ext)
            new_filename = f"{cleaned_name}{file_ext}"
        
        new_path = get_unique_filename(DOWNLOAD_DIR, new_filename)
        temp_path.rename(new_path)
        
        if db.get_metadata_enabled(user_id):
            await status_msg.edit_text("🔄 Adding metadata...", reply_markup=control_keyboard)
            new_path = add_metadata_to_video(new_path, user_id)
            file_size = new_path.stat().st_size
            video_duration = get_video_duration_ffprobe(str(new_path)) or video_duration
        
        if USER_CANCEL.get(user_id, False):
            USER_CANCEL[user_id] = False
            raise asyncio.CancelledError()
        
        await status_msg.edit_text(
            f"✅ **Download complete!**\n\n"
            f"📁 **File:** `{new_path.name}`\n"
            f"📊 **Size:** {format_file_size(file_size)}\n"
            f"⏱️ **Duration:** {format_duration(video_duration)}\n\n"
            f"📤 **Uploading...** (0%)",
            reply_markup=control_keyboard
        )
        
        caption = generate_bold_caption(new_path.name)
        
        task = asyncio.create_task(show_continuous_action(client, chat_id, "upload_video", 300))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        
        await client.send_video(
            chat_id=chat_id,
            video=str(new_path),
            thumb=thumb_path,
            caption=caption,
            duration=video_duration,
            width=video_dimensions[0],
            height=video_dimensions[1],
            supports_streaming=True,
            progress=upload_progress,
            progress_args=(status_msg, "Uploading")
        )
        
        db.update_stats(file_size, user_id)
        
        last_progress_info.pop(status_msg.id, None)
        
        success_text = f"🎬 **Upload Complete!**\n\n✅ File: `{new_path.name}`\n✅ Size: {format_file_size(file_size)}\n✅ Duration: {format_duration(video_duration)}"
        if SERIAL_CHANNEL:
            success_text += f"\n\n📢 **Join:** {SERIAL_CHANNEL}"
        
        await status_msg.edit_text(success_text)
        
        new_path.unlink()
        
    except asyncio.CancelledError:
        logger.info(f"User {user_id} cancelled job")
        if temp_path and temp_path.exists():
            temp_path.unlink()
        if new_path and new_path.exists():
            new_path.unlink()
        USER_CANCEL.pop(user_id, None)
        try:
            await status_msg.edit_text("✅ Job cancelled.")
        except:
            pass
        return
    except Exception as e:
        logger.error(f"Error in process_single_video for user {user_id}: {e}", exc_info=True)
        FAILED_JOBS[user_id] = {
            "type": "single",
            "original_filename": original_filename,
            "file_size": file_size,
            "video_dimensions": video_dimensions,
            "video_duration": video_duration,
            "mime_type": data.get("mime_type"),
            "display_name": display_name,
            "chat_id": chat_id,
            "message_id": message.id,
            "error": str(e)
        }
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Retry", callback_data=f"retry_{user_id}_single")
        ]])
        try:
            await status_msg.edit_text(f"❌ Error: {str(e)[:200]}", reply_markup=keyboard)
        except:
            pass
    finally:
        USER_PAUSED_JOBS.pop(user_id, None)
        USER_PAUSE_EVENTS.pop(user_id, None)

# ===== CALLBACK QUERY HANDLER =====
@app.on_callback_query(filters.regex(r"^retry_(\d+)_single$"))
async def retry_callback(client: Client, callback_query: CallbackQuery):
    """Handle retry button clicks"""
    import re
    match = re.match(r"^retry_(\d+)_single$", callback_query.data)
    if not match:
        await callback_query.answer("Invalid request", show_alert=True)
        return
    
    user_id = int(match.group(1))
    
    if callback_query.from_user.id != user_id:
        await callback_query.answer("This is not your job!", show_alert=True)
        return
    
    if user_id not in FAILED_JOBS:
        await callback_query.answer("No failed job data found!", show_alert=True)
        return
    
    job_data = FAILED_JOBS[user_id]
    
    chat_id = job_data.get("chat_id")
    msg_id = job_data.get("message_id")
    try:
        original_msg = await client.get_messages(chat_id, msg_id)
        if not original_msg or not (original_msg.video or original_msg.document):
            raise ValueError("Original message not found or not a video")
        
        queue_data = {
            "user_id": user_id,
            "original_filename": job_data["original_filename"],
            "file_size": job_data["file_size"],
            "video_dimensions": job_data["video_dimensions"],
            "video_duration": job_data["video_duration"],
            "mime_type": job_data["mime_type"],
            "display_name": job_data.get("display_name", job_data["original_filename"]),
            "operation": "single",
            "chat_id": chat_id,
            "message_id": msg_id,
            "timestamp": time.time()
        }
        UPLOAD_QUEUE.append((original_msg, queue_data))
        await callback_query.message.edit_text("✅ Job re-added to queue. Check /status")
        logger.info(f"User {user_id} retried single job")
    except Exception as e:
        await callback_query.message.edit_text(f"❌ Failed to retry: {e}")
        logger.error(f"Retry error: {e}")
    
    FAILED_JOBS.pop(user_id, None)
    await callback_query.answer()

@app.on_callback_query(filters.regex(r"^lang_(en|hi)$"))
async def language_callback(client: Client, callback_query: CallbackQuery):
    """Handle language selection"""
    user_id = callback_query.from_user.id
    lang_code = callback_query.data.split('_')[1]

    db.set_user_lang(user_id, lang_code)
    USER_LANG[user_id] = lang_code

    await callback_query.answer()
    await callback_query.message.edit_text(
        get_text(user_id, 'lang_set'),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]])
    )

@app.on_callback_query(filters.regex(r"^dup_override_(\d+)$"))
async def duplicate_override_callback(client: Client, callback_query: CallbackQuery):
    """Handle duplicate override button click"""
    import re
    match = re.match(r"^dup_override_(\d+)$", callback_query.data)
    if not match:
        await callback_query.answer("Invalid request", show_alert=True)
        return
    
    user_id = int(match.group(1))
    
    if callback_query.from_user.id != user_id:
        await callback_query.answer("This is not your file!", show_alert=True)
        return
    
    key = f"dup_{user_id}"
    if key not in FAILED_JOBS:
        await callback_query.answer("No duplicate job data found!", show_alert=True)
        return
    
    job_data = FAILED_JOBS[key]
    
    queue_data = {
        "user_id": user_id,
        "original_filename": job_data["original_filename"],
        "file_size": job_data["file_size"],
        "video_dimensions": job_data["video_dimensions"],
        "video_duration": job_data["video_duration"],
        "mime_type": job_data["mime_type"],
        "display_name": job_data["display_name"],
        "operation": "single",
        "chat_id": job_data["chat_id"],
        "message_id": job_data["message_id"],
        "force_duplicate": True,
        "temp_path": job_data["temp_path"],
        "timestamp": time.time()
    }
    
    try:
        original_msg = await client.get_messages(job_data["chat_id"], job_data["message_id"])
        if not original_msg or not (original_msg.video or original_msg.document):
            raise ValueError("Original message not found or not a video")
        
        UPLOAD_QUEUE.append((original_msg, queue_data))
        await callback_query.message.edit_text("✅ File will be uploaded despite duplicate. Check /status")
        logger.info(f"User {user_id} overrode duplicate check")
    except Exception as e:
        await callback_query.message.edit_text(f"❌ Failed to override: {e}")
        logger.error(f"Duplicate override error: {e}")
    
    FAILED_JOBS.pop(key, None)
    await callback_query.answer()

@app.on_callback_query(filters.regex(r"^meta_(on|off)$"))
async def metadata_callback(client: Client, callback_query: CallbackQuery):
    """Handle metadata toggle"""
    user_id = callback_query.from_user.id
    action = callback_query.data.split('_')[1]
    
    enabled = (action == 'on')
    db.set_metadata_enabled(user_id, enabled)
    
    await callback_query.answer(f"Metadata {'enabled' if enabled else 'disabled'}")
    
    status_text = "**Metadata Settings**\n\n"
    status_text += f"Current: {'✅ Enabled' if enabled else '❌ Disabled'}\n\nChoose option:"
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ ON" if enabled else "⚪ ON", callback_data="meta_on"),
            InlineKeyboardButton("❌ OFF" if not enabled else "⚪ OFF", callback_data="meta_off")
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
    ])
    
    await callback_query.message.edit_text(status_text, reply_markup=keyboard)

# ===== PAUSE/RESUME BUTTON CALLBACKS =====
@app.on_callback_query(filters.regex(r"^pause_(\d+)$"))
async def pause_button_callback(client: Client, callback_query: CallbackQuery):
    """Handle pause button click during processing"""
    user_id = int(callback_query.data.split('_')[1])
    
    if callback_query.from_user.id != user_id:
        await callback_query.answer("❌ Yeh aapka job nahi hai!", show_alert=True)
        return

    if user_id not in USER_PAUSE_EVENTS:
        USER_PAUSE_EVENTS[user_id] = asyncio.Event()
        USER_PAUSE_EVENTS[user_id].set()

    if USER_PAUSED_JOBS.get(user_id, False):
        await callback_query.answer("⏸️ Job already paused!", show_alert=True)
    else:
        USER_PAUSED_JOBS[user_id] = True
        USER_PAUSE_EVENTS[user_id].clear()
        await callback_query.answer("⏸️ Job paused.")
        try:
            await callback_query.message.edit_text(
                callback_query.message.text + "\n\n⏸️ **Paused**",
                reply_markup=callback_query.message.reply_markup
            )
        except:
            pass

@app.on_callback_query(filters.regex(r"^resume_(\d+)$"))
async def resume_button_callback(client: Client, callback_query: CallbackQuery):
    """Handle resume button click during processing"""
    user_id = int(callback_query.data.split('_')[1])
    
    if callback_query.from_user.id != user_id:
        await callback_query.answer("❌ Yeh aapka job nahi hai!", show_alert=True)
        return

    if not USER_PAUSED_JOBS.get(user_id, False):
        await callback_query.answer("▶️ Job is already running!", show_alert=True)
    else:
        USER_PAUSED_JOBS[user_id] = False
        if user_id in USER_PAUSE_EVENTS:
            USER_PAUSE_EVENTS[user_id].set()
        await callback_query.answer("▶️ Job resumed.")
        try:
            new_text = callback_query.message.text.replace("\n\n⏸️ **Paused**", "")
            await callback_query.message.edit_text(new_text, reply_markup=callback_query.message.reply_markup)
        except:
            pass

@app.on_callback_query(filters.regex(r"^cancel_(\d+)$"))
async def cancel_callback(client: Client, callback_query: CallbackQuery):
    """Handle cancel button click during processing"""
    user_id = int(callback_query.data.split('_')[1])
    
    if callback_query.from_user.id != user_id:
        await callback_query.answer("❌ Yeh aapka job nahi hai!", show_alert=True)
        return
    
    USER_CANCEL[user_id] = True
    await callback_query.answer("⏸️ Cancelling...", show_alert=False)
    
    try:
        await callback_query.message.edit_text("⏳ **Cancelling current operation...**")
    except:
        pass
    
    logger.info(f"User {user_id} requested cancellation")

# ===== MENU CALLBACKS =====
@app.on_callback_query(filters.regex(r"^main_menu$"))
async def main_menu_callback(client: Client, callback_query: CallbackQuery):
    """Return to main menu"""
    user_id = callback_query.from_user.id
    await callback_query.answer()
    await callback_query.message.edit_text("📌 **Main Menu**", reply_markup=get_main_menu_keyboard(user_id))

@app.on_callback_query(filters.regex(r"^settings_menu$"))
async def settings_menu_callback(client: Client, callback_query: CallbackQuery):
    """Show settings menu"""
    user_id = callback_query.from_user.id
    await callback_query.answer()
    text = "⚙️ **Settings**\n\nManage your preferences:"
    await callback_query.message.edit_text(text, reply_markup=get_settings_keyboard(user_id))

@app.on_callback_query(filters.regex(r"^language_menu$"))
async def language_menu_callback(client: Client, callback_query: CallbackQuery):
    """Show language selection menu"""
    user_id = callback_query.from_user.id
    await callback_query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
         InlineKeyboardButton("🇮🇳 Hinglish", callback_data="lang_hi")],
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
    ])
    await callback_query.message.edit_text("Please select your language / भाषा चुनें:", reply_markup=keyboard)

@app.on_callback_query(filters.regex(r"^metadata_menu$"))
async def metadata_menu_callback(client: Client, callback_query: CallbackQuery):
    """Show metadata settings"""
    user_id = callback_query.from_user.id
    await callback_query.answer()
    current_status = db.get_metadata_enabled(user_id)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ ON" if current_status else "⚪ ON", callback_data="meta_on"),
            InlineKeyboardButton("❌ OFF" if not current_status else "⚪ OFF", callback_data="meta_off")
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
    ])
    status_text = "**Metadata Settings**\n\n"
    status_text += f"Current: {'✅ Enabled' if current_status else '❌ Disabled'}\n\n"
    status_text += "When enabled, bot adds:\n"
    status_text += "• Title: Encoded By :- @MNA_3786\n"
    status_text += "• Author: @MNA_3786\n"
    status_text += "• Description: Subtitled By :- @EntertainmentTadka786\n"
    status_text += "• Comment: Audio/Video credits\n\n"
    status_text += "Choose option:"
    await callback_query.message.edit_text(status_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex(r"^channels_menu$"))
async def channels_menu_callback(client: Client, callback_query: CallbackQuery):
    """Show channels links"""
    await callback_query.answer()
    await callback_query.message.edit_text("📢 **Our Channels**\n\nJoin and stay updated!", reply_markup=get_channels_keyboard())

@app.on_callback_query(filters.regex(r"^help_menu$"))
async def help_menu_callback(client: Client, callback_query: CallbackQuery):
    """Show help menu"""
    await callback_query.answer()
    await callback_query.message.delete()
    await help_command(client, callback_query.message)

@app.on_callback_query(filters.regex(r"^refresh_menu$"))
async def refresh_menu_callback(client: Client, callback_query: CallbackQuery):
    """Refresh the main menu"""
    user_id = callback_query.from_user.id
    await callback_query.answer("Refreshed!")
    await callback_query.message.edit_text("📌 **Main Menu**", reply_markup=get_main_menu_keyboard(user_id))

@app.on_callback_query(filters.regex(r"^mode_single$"))
async def mode_single_callback(client: Client, callback_query: CallbackQuery):
    """Show single video guide"""
    user_id = callback_query.from_user.id
    await callback_query.answer()
    guide = f"""
🎬 **Single Video Mode Guide:**

1. **Set Thumbnail:**
   - Send any photo (100% QUALITY PRESERVED)
   - Or use `/thumb` command

2. **Upload Video:**
   - Send video file (max {format_file_size(MAX_FILE_SIZE)})
   - Supported: MP4, MKV, AVI, MOV

3. **Customize (Optional):**
   - `/filerename Movie Name` → Temporary
   - `/filerename permanent Template` → Permanent
   - `/myformat` → Check settings

4. **Wait & Done:**
   - Bot processes automatically
   - Check status with `/status`
   - **Live speed display** shows MBps
   - **Duplicate detection** prevents re-upload
   - **Cancel button** available
"""
    if SERIAL_CHANNEL:
        guide += f"\n\n📢 **Join:** {SERIAL_CHANNEL}"
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]])
    await callback_query.message.edit_text(guide, reply_markup=keyboard)

@app.on_callback_query(filters.regex(r"^thumb_settings$"))
async def thumb_settings_callback(client: Client, callback_query: CallbackQuery):
    """Thumbnail settings submenu"""
    user_id = callback_query.from_user.id
    await callback_query.answer()
    if user_id in USER_THUMB:
        text = "🖼️ **Thumbnail is set.**\n\nUse `/clearthumb` to remove it."
    else:
        text = "🖼️ **No thumbnail set.**\n\nSend a photo or use `/thumb` to set one."
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="settings_menu")]])
    await callback_query.message.edit_text(text, reply_markup=keyboard)

@app.on_callback_query(filters.regex(r"^permname_settings$"))
async def permname_settings_callback(client: Client, callback_query: CallbackQuery):
    """Permanent name settings submenu"""
    user_id = callback_query.from_user.id
    await callback_query.answer()
    if user_id in USER_PERMANENT_NAMES:
        text = f"📛 **Permanent name template:**\n`{USER_PERMANENT_NAMES[user_id]}`\n\nUse `/filerename permanent reset` to clear it."
    else:
        text = "📛 **No permanent name set.**\n\nUse `/filerename permanent Template` to set one."
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="settings_menu")]])
    await callback_query.message.edit_text(text, reply_markup=keyboard)

@app.on_callback_query(filters.regex(r"^status$"))
async def status_callback(client: Client, callback_query: CallbackQuery):
    """Show status from menu"""
    await callback_query.answer()
    await callback_query.message.delete()
    await status_command(client, callback_query.message)

# ===== STARTUP CLEANUP =====
def startup_cleanup():
    """Cleanup old files on startup"""
    try:
        cutoff = time.time() - 86400
        
        for folder in [DOWNLOAD_DIR, THUMB_DIR, OUTPUT_DIR]:
            if folder.exists():
                for item in folder.glob('*'):
                    try:
                        if item.is_file() and item.stat().st_mtime < cutoff:
                            item.unlink()
                            logger.info(f"Cleaned up old file: {item}")
                    except Exception as e:
                        logger.error(f"Error cleaning {item}: {e}")
        
        db.clear_old_backups(24)
        logger.info("Startup cleanup completed")
    except Exception as e:
        logger.error(f"Startup cleanup error: {e}")

# ===== MAIN ENTRY POINT =====
if __name__ == "__main__":
    print("=" * 60)
    print("🤖 VIDEO UPLOADER BOT - SINGLE MODE ONLY")
    print("=" * 60)
    print(f"👤 Owner ID: {BOT_OWNER_ID}")
    print(f"📊 Max File Size: {format_file_size(MAX_FILE_SIZE)}")
    print(f"📥 Max Queue Size: {MAX_QUEUE_SIZE}")
    print(f"⏱️ Rate Limit: {RATE_LIMIT_SECONDS}s")
    print("=" * 60)
    
    print("\n📢 **Configured Channels:**")
    if MAIN_CHANNEL:
        print(f"  • Main: {MAIN_CHANNEL}")
    if REQUEST_CHANNEL:
        print(f"  • Request: {REQUEST_CHANNEL}")
    if PRINTS_CHANNEL:
        print(f"  • Theater: {PRINTS_CHANNEL}")
    if BACKUP_CHANNEL:
        print(f"  • Backup: {BACKUP_CHANNEL}")
    if SERIAL_CHANNEL:
        print(f"  • Serial: {SERIAL_CHANNEL}")
    
    print("=" * 60)
    print("\n✨ **FEATURES:**")
    print("✅ Single video upload mode")
    print("✅ Thumbnail 100% quality")
    print("✅ Custom filename")
    print("✅ Queue system")
    print("✅ Live speed display (MBps)")
    print("✅ Pause/Resume with buttons")
    print("✅ Retry failed jobs")
    print("✅ Duplicate detection")
    print("✅ Cancel button")
    print("✅ Metadata support")
    print("✅ Language support (English/Hinglish)")
    print("✅ Premium menu UI")
    print("=" * 60)
    
    startup_cleanup()
    
    logger.info("Bot starting up...")
    print("\n✅ Bot is running...")
    print("🛑 Press Ctrl+C to stop\n")
    
    try:
        app.run()
    except KeyboardInterrupt:
        print("\n\n🛑 Bot stopped by user")
        logger.info("Bot stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Bot crashed: {e}")