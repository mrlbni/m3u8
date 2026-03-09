#!/usr/bin/env python3
# ============================================
# M3U8 TELEGRAM BOT - OPTIMIZED FOR RENDER
# ============================================
# File: m3u8_bot.py
# Run:  python m3u8_bot.py
# ============================================

import os
import sys
import logging
import time
import shutil
import asyncio
import warnings
import subprocess
import math
import json
import signal
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from typing import Optional, Tuple, Dict, List, Any
import threading

# ============================================
# FIX: File naam conflict with m3u8 library
# ============================================
_script_dir = os.path.dirname(os.path.abspath(__file__))
_saved_path = sys.path.copy()
sys.path = [p for p in sys.path if os.path.abspath(p) != os.path.abspath(_script_dir)]

try:
    import m3u8 as m3u8_lib  # Real library from site-packages
except ImportError:
    print("ERROR: m3u8 library not installed. Run: pip install m3u8")
    sys.exit(1)

sys.path = _saved_path  # Restore path
# ============================================

# Flask for web server (Render health checks)
from flask import Flask, jsonify
from flask_cors import CORS
import requests
import nest_asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from pyrogram import Client

warnings.filterwarnings('ignore')
nest_asyncio.apply()

# ============================================
# CONFIGURATION - Environment Variables for Render
# ============================================
# Get from environment variables (set in Render dashboard)
API_ID = int(os.environ.get("API_ID", "0"))  # Default to 0 to catch missing
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PORT = int(os.environ.get("PORT", 5000))  # Render assigns port via environment
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "")

# Check required environment variables
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN environment variable not set!")
    sys.exit(1)

if not API_ID or API_ID == 0:
    print("ERROR: API_ID environment variable not set or invalid!")
    sys.exit(1)

if not API_HASH:
    print("ERROR: API_HASH environment variable not set!")
    sys.exit(1)

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = os.path.join(BASE_DIR, "downloads")
TEMP_FOLDER = os.path.join(BASE_DIR, "temp")
LOG_FOLDER = os.path.join(BASE_DIR, "logs")

# Create directories
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)
os.makedirs(LOG_FOLDER, exist_ok=True)

# ============================================
# LOGGING CONFIGURATION
# ============================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(os.path.join(LOG_FOLDER, 'bot.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# BOT CONFIGURATION
# ============================================
MAX_WORKERS = 15
MAX_QUEUE = 100
MAX_FILE_SIZE = 1900 * 1024 * 1024  # 1.9GB (for split into <2GB parts)
TASK_TIMEOUT = 3600  # 60 min per URL
CLEANUP_INTERVAL = 300  # Clean old files every 5 minutes

# ============================================
# LANGUAGE FLAGS
# ============================================
LANG_FLAGS = {
    'hi':'🇮🇳 Hindi','hin':'🇮🇳 Hindi','hindi':'🇮🇳 Hindi',
    'ja':'🇯🇵 Japanese','jpn':'🇯🇵 Japanese','japanese':'🇯🇵 Japanese',
    'en':'🇺🇸 English','eng':'🇺🇸 English','english':'🇺🇸 English',
    'ko':'🇰🇷 Korean','kor':'🇰🇷 Korean','korean':'🇰🇷 Korean',
    'ta':'🇮🇳 Tamil','tam':'🇮🇳 Tamil','tamil':'🇮🇳 Tamil',
    'te':'🇮🇳 Telugu','tel':'🇮🇳 Telugu','telugu':'🇮🇳 Telugu',
    'bn':'🇮🇳 Bengali','ben':'🇮🇳 Bengali','bengali':'🇮🇳 Bengali',
    'mr':'🇮🇳 Marathi','mar':'🇮🇳 Marathi','marathi':'🇮🇳 Marathi',
    'ml':'🇮🇳 Malayalam','mal':'🇮🇳 Malayalam','malayalam':'🇮🇳 Malayalam',
    'kn':'🇮🇳 Kannada','kan':'🇮🇳 Kannada','kannada':'🇮🇳 Kannada',
    'gu':'🇮🇳 Gujarati','guj':'🇮🇳 Gujarati','gujarati':'🇮🇳 Gujarati',
    'pa':'🇮🇳 Punjabi','pan':'🇮🇳 Punjabi','punjabi':'🇮🇳 Punjabi',
    'ur':'🇵🇰 Urdu','urd':'🇵🇰 Urdu','urdu':'🇵🇰 Urdu',
    'zh':'🇨🇳 Chinese','fr':'🇫🇷 French','de':'🇩🇪 German',
    'es':'🇪🇸 Spanish','pt':'🇵🇹 Portuguese','ru':'🇷🇺 Russian',
    'ar':'🇸🇦 Arabic','it':'🇮🇹 Italian','th':'🇹🇭 Thai',
}

# ============================================
# FLASK WEB SERVER FOR RENDER HEALTH CHECKS
# ============================================
flask_app = Flask(__name__)
CORS(flask_app)  # Enable CORS for all routes

# Bot status tracking
bot_start_time = time.time()
bot_status = {
    'status': 'starting',
    'start_time': bot_start_time,
    'uptime': 0,
    'active_downloads': 0,
    'queue_size': 0,
    'total_downloads': 0,
    'total_uploads': 0,
    'total_errors': 0,
    'memory_usage': 0
}

@flask_app.route('/')
def home():
    """Root endpoint - shows bot is running"""
    return jsonify({
        'status': 'online',
        'bot': 'M3U8 Telegram Bot',
        'version': '2.0',
        'message': 'Bot is running!'
    })

@flask_app.route('/health')
def health():
    """Health check endpoint for Render"""
    uptime = time.time() - bot_start_time
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'uptime_seconds': uptime,
        'uptime_human': format_time(uptime),
        'active_downloads': bot_status['active_downloads'],
        'queue_size': bot_status['queue_size']
    })

@flask_app.route('/status')
def status():
    """Detailed status endpoint"""
    global bot_status
    bot_status['uptime'] = time.time() - bot_start_time
    
    # Get system info if psutil is available
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        bot_status['memory_usage'] = memory_info.rss / 1024 / 1024  # MB
    except ImportError:
        bot_status['memory_usage'] = 0
    
    return jsonify(bot_status)

@flask_app.route('/stats')
def stats():
    """Statistics endpoint"""
    return jsonify({
        'downloads': bot_status['total_downloads'],
        'uploads': bot_status['total_uploads'],
        'errors': bot_status['total_errors'],
        'active': bot_status['active_downloads'],
        'queue': bot_status['queue_size']
    })

def run_flask():
    """Run Flask server in a separate thread"""
    logger.info(f"Starting Flask server on port {PORT}")
    flask_app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)

# ============================================
# UTILITY FUNCTIONS
# ============================================
def format_bytes(b):
    """Format bytes to human readable"""
    if b == 0: return "0 B"
    for u in ['B', 'KB', 'MB', 'GB', 'TB']:
        if b < 1024.0: return f"{b:.2f} {u}"
        b /= 1024.0
    return f"{b:.2f} PB"

def format_time(s):
    """Format seconds to human readable"""
    if s < 0: return "..."
    if s < 60: return f"{int(s)}s"
    elif s < 3600: return f"{int(s//60)}m {int(s%60)}s"
    else: return f"{int(s//3600)}h {int((s%3600)//60)}m"

def progress_bar(p, l=15):
    """Create a text progress bar"""
    f = int(l * p / 100)
    return '█' * f + '░' * (l - f)

def get_lang_display(language, name):
    """Get language display with flag"""
    if language:
        k = language.lower().strip()
        if k in LANG_FLAGS: return LANG_FLAGS[k]
    if name:
        n = name.lower().strip()
        if n in LANG_FLAGS: return LANG_FLAGS[n]
        for key, val in LANG_FLAGS.items():
            if key in n: return val
        return f"🔊 {name}"
    return f"🔊 {language}" if language else "🔊 Unknown"

def is_hindi_track(t):
    """Check if track is Hindi"""
    l = (t.get('language') or '').lower()
    n = (t.get('name') or '').lower()
    return l in ('hi', 'hin', 'hindi') or 'hindi' in n

# ============================================
# CANCEL SYSTEM
# ============================================
cancel_flags = {}

def is_cancelled(chat_id):
    return cancel_flags.get(chat_id, 'none') != 'none'

def is_all_cancelled(chat_id):
    return cancel_flags.get(chat_id, 'none') == 'all'

# ============================================
# QUEUE SYSTEM
# ============================================
class TaskQueue:
    def __init__(self, mc=15, mq=100):
        self.mc = mc
        self.mq = mq
        self.active = {}
        self.waiting = deque()
        self.lock = asyncio.Lock()

    async def add_task(self, cid, ti):
        async with self.lock:
            if cid in self.active:
                return False, "❌ You already have an active download. Use /allcancel to cancel first."
            if len(self.waiting) >= self.mq:
                return False, f"❌ Queue full ({self.mq})."
            if len(self.active) < self.mc:
                self.active[cid] = ti
                return True, f"✅ Starting... (Active: {len(self.active)}/{self.mc})"
            else:
                self.waiting.append((cid, ti))
                return True, f"⏳ Queue position: {len(self.waiting)}"

    async def complete_task(self, cid):
        async with self.lock:
            self.active.pop(cid, None)
            if self.waiting and len(self.active) < self.mc:
                nc, nt = self.waiting.popleft()
                self.active[nc] = nt
                return nc, nt
            return None, None

    async def get_status(self):
        async with self.lock:
            return {'active': len(self.active), 'queue': len(self.waiting), 'max': self.mc}

    async def get_position(self, cid):
        async with self.lock:
            if cid in self.active: return 0
            for i, (c, _) in enumerate(self.waiting):
                if c == cid: return i + 1
            return -1

    async def remove_all(self, cid):
        async with self.lock:
            self.active.pop(cid, None)
            self.waiting = deque([(c, t) for c, t in self.waiting if c != cid])

task_queue = TaskQueue(mc=MAX_WORKERS, mq=MAX_QUEUE)
user_data_store = {}
pyro_client = None

# ============================================
# FILE CLEANUP FUNCTIONS
# ============================================
def cleanup_file(fp):
    """Delete a single file and its parts"""
    try:
        if fp and os.path.exists(fp):
            os.remove(fp)
            logger.info(f"Cleaned file: {fp}")
        if fp:
            bn = os.path.splitext(fp)[0]
            d = os.path.dirname(fp)
            if os.path.exists(d):
                for f in os.listdir(d):
                    if f.startswith(os.path.basename(bn)):
                        try:
                            os.remove(os.path.join(d, f))
                            logger.info(f"Cleaned part: {f}")
                        except:
                            pass
    except Exception as e:
        logger.error(f"Cleanup error for {fp}: {e}")

def cleanup_user_temp(chat_id):
    """Delete ALL temp folders for this user"""
    try:
        if os.path.exists(TEMP_FOLDER):
            for folder in os.listdir(TEMP_FOLDER):
                if folder.startswith(str(chat_id)):
                    fp = os.path.join(TEMP_FOLDER, folder)
                    shutil.rmtree(fp, ignore_errors=True)
                    logger.info(f"Cleaned temp folder for user {chat_id}")
    except Exception as e:
        logger.error(f"User temp cleanup error: {e}")

def cleanup_user_downloads(chat_id):
    """Delete ALL download files for this user"""
    try:
        if os.path.exists(DOWNLOAD_FOLDER):
            for f in os.listdir(DOWNLOAD_FOLDER):
                if str(chat_id) in f:
                    try:
                        os.remove(os.path.join(DOWNLOAD_FOLDER, f))
                        logger.info(f"Cleaned download: {f}")
                    except:
                        pass
    except Exception as e:
        logger.error(f"User downloads cleanup error: {e}")

def full_cleanup(chat_id, file_path=None):
    """Complete cleanup: specific file + user temps"""
    if file_path:
        cleanup_file(file_path)
    cleanup_user_temp(chat_id)

def periodic_cleanup():
    """Cleanup old files periodically"""
    while True:
        time.sleep(CLEANUP_INTERVAL)
        try:
            logger.info("Running periodic cleanup...")
            # Clean temp files older than 1 hour
            now = time.time()
            for folder in [TEMP_FOLDER, DOWNLOAD_FOLDER]:
                if os.path.exists(folder):
                    for item in os.listdir(folder):
                        item_path = os.path.join(folder, item)
                        if os.path.isfile(item_path):
                            # Delete files older than 1 hour
                            if now - os.path.getmtime(item_path) > 3600:
                                os.remove(item_path)
                                logger.info(f"Cleaned old file: {item}")
                        elif os.path.isdir(item_path):
                            # Delete folders older than 1 hour
                            if now - os.path.getmtime(item_path) > 3600:
                                shutil.rmtree(item_path, ignore_errors=True)
                                logger.info(f"Cleaned old folder: {item}")
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")

# ============================================
# VIDEO METADATA FUNCTIONS
# ============================================
def get_meta(v):
    """Get video metadata using ffprobe"""
    try:
        r = subprocess.run([
            'ffprobe', '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height,duration',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1', v
        ], capture_output=True, text=True, timeout=30)
        
        w = h = d = 0
        for line in r.stdout.split('\n'):
            if 'width=' in line:
                w = int(line.split('=')[1])
            elif 'height=' in line:
                h = int(line.split('=')[1])
            elif 'duration=' in line:
                try:
                    d = int(float(line.split('=')[1]))
                except:
                    pass
        return w, h, d
    except Exception as e:
        logger.error(f"Metadata error: {e}")
        return 0, 0, 0

def make_thumb(v, o):
    """Create thumbnail from video"""
    try:
        _, _, d = get_meta(v)
        off = min(5, d // 4) if d > 0 else 5
        
        subprocess.run([
            'ffmpeg', '-y', '-ss', str(off), '-i', v, '-vframes', '1',
            '-vf', 'scale=320:-1', '-q:v', '2', o
        ], capture_output=True, timeout=30)
        
        if os.path.exists(o) and os.path.getsize(o) > 100:
            return o
        return None
    except Exception as e:
        logger.error(f"Thumbnail error: {e}")
        return None

def split_video(v, max_size=MAX_FILE_SIZE):
    """Split video into parts if too large"""
    fs = os.path.getsize(v)
    if fs <= max_size:
        return [v]
    
    np2 = math.ceil(fs / max_size)
    _, _, d = get_meta(v)
    
    if d == 0:
        return [v]
    
    cd = d // np2
    parts = []
    bn = os.path.splitext(v)[0]
    
    for i in range(np2):
        st = i * cd
        pd = d - st if i == np2 - 1 else cd
        pn = f"{bn}_part{i+1}.mp4"
        
        subprocess.run([
            'ffmpeg', '-y', '-ss', str(st), '-i', v, '-t', str(pd),
            '-c', 'copy', '-movflags', '+faststart',
            '-avoid_negative_ts', 'make_zero', pn
        ], capture_output=True, timeout=600)
        
        if os.path.exists(pn) and os.path.getsize(pn) > 1000:
            parts.append(pn)
            logger.info(f"Created part {i+1}: {pn}")
    
    return parts if parts else [v]

# ============================================
# URL PARSING FUNCTIONS
# ============================================
def extract_urls(text):
    """Extract URLs from text"""
    urls = []
    for part in text.replace('\n', ' ').split():
        part = part.strip()
        if part.startswith(('http://', 'https://')) and part not in urls:
            urls.append(part)
    return urls

def get_video_name(url, idx=0, total=1):
    """Extract video name from URL"""
    try:
        parts = unquote(urlparse(url).path).strip('/').split('/')
        skip = {
            'hls', 'video', 'stream', 'media', 'content',
            'h264_high', 'h264_low', 'master', 'index',
            'playlist', 'manifest', '0', '1', '2', '3'
        }
        
        for part in reversed(parts):
            name = part.replace('.m3u8', '').replace('.m3u', '')
            if name and len(name) > 3 and name.lower() not in skip:
                clean = "".join(c if c.isalnum() or c in '-_' else '_' for c in name)
                return clean[:40]
    except:
        pass
    
    return f"Video_{idx+1}" if total > 1 else "video"

def find_quality(qs, target_h):
    """Find quality by height"""
    if not qs:
        return None
    for q in qs:
        if q['height'] == target_h:
            return q
    return min(qs, key=lambda q: abs(q['height'] - target_h))

def find_audio(ats, lang, name=''):
    """Find audio track by language"""
    if not ats:
        return None
    if not lang and not name:
        return ats[0]
    
    for t in ats:
        tl = (t.get('language') or '').lower()
        tn = (t.get('name') or '').lower()
        if lang and (lang.lower() in tl or lang.lower() == tl):
            return t
        if name and (name.lower() in tn or name.lower() == tn):
            return t
    
    return ats[0]

# ============================================
# M3U8 PARSER
# ============================================
async def parse_m3u8(url):
    """Parse M3U8 playlist"""
    try:
        logger.info(f"Parsing M3U8: {url}")
        playlist = m3u8_lib.load(url)
        
        if playlist.is_variant:
            # Parse audio tracks
            aat = []
            seen = set()
            
            if hasattr(playlist, 'media') and playlist.media:
                for media in playlist.media:
                    if media.type == 'AUDIO' and media.uri:
                        au = urljoin(url, media.uri)
                        if au in seen:
                            continue
                        seen.add(au)
                        
                        ti = {
                            'group_id': media.group_id or 'default',
                            'language': getattr(media, 'language', None) or '',
                            'name': getattr(media, 'name', None) or '',
                            'url': au,
                            'display': get_lang_display(
                                getattr(media, 'language', None),
                                getattr(media, 'name', None)
                            ),
                        }
                        aat.append(ti)
                        logger.info(f"Audio track: {ti['display']}")
            
            logger.info(f"Total audio tracks: {len(aat)}")
            
            # Parse video qualities
            qs = []
            for p in playlist.playlists:
                res = p.stream_info.resolution
                bw = p.stream_info.bandwidth
                
                if res:
                    qs.append({
                        'name': f"{res[1]}p",
                        'resolution': f"{res[0]}x{res[1]}",
                        'bandwidth': bw,
                        'url': urljoin(url, p.uri),
                        'width': res[0],
                        'height': res[1],
                        'audio_url': None
                    })
            
            qs.sort(key=lambda x: x['height'])
            logger.info(f"Found {len(qs)} qualities")
            
            return {'qualities': qs, 'audio_tracks': aat}
        else:
            # Single stream
            return {
                'qualities': [{
                    'name': 'default',
                    'resolution': 'Unknown',
                    'bandwidth': 0,
                    'url': url,
                    'width': 0,
                    'height': 0,
                    'audio_url': None
                }],
                'audio_tracks': []
            }
            
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return None

# ============================================
# DOWNLOADER CLASS
# ============================================
class Downloader:
    def __init__(self, url, qname, cid, callback, audio_url=None, vname="video"):
        self.url = url
        self.audio_url = audio_url
        self.qname = qname
        self.cid = cid
        self.cb = callback
        self.vname = vname
        
        # Setup session
        self.session = requests.Session()
        ad = requests.adapters.HTTPAdapter(
            pool_connections=30,
            pool_maxsize=30,
            max_retries=3
        )
        self.session.mount('https://', ad)
        self.session.mount('http://', ad)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        
        # Create temp directory
        os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
        self.tmp = os.path.join(TEMP_FOLDER, f"{cid}_{qname}_{int(time.time())}")
        if os.path.exists(self.tmp):
            shutil.rmtree(self.tmp)
        os.makedirs(self.tmp)
        
        self.vdir = os.path.join(self.tmp, "v")
        os.makedirs(self.vdir, exist_ok=True)
        
        self.adir = os.path.join(self.tmp, "a")
        if self.audio_url:
            os.makedirs(self.adir, exist_ok=True)
        
        # Output file
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_vname = "".join(c for c in vname if c.isalnum() or c in ' -_').strip()
        self.out = os.path.join(DOWNLOAD_FOLDER, f"{safe_vname}_{qname}_{ts}.mp4")

    def _parse_pl(self, url, sd):
        """Parse playlist and get segments"""
        pl = m3u8_lib.load(url)
        bu = url.rsplit('/', 1)[0] + '/'
        hi = False
        
        # Check for init segment
        if pl.segment_map:
            for sm in pl.segment_map:
                if sm.uri:
                    iu = sm.uri if sm.uri.startswith("http") else urljoin(bu, sm.uri)
                    r = self.session.get(iu, timeout=30)
                    with open(os.path.join(sd, "init.mp4"), 'wb') as f:
                        f.write(r.content)
                    hi = True
                    break
        
        # Get segments
        segs = []
        for sg in pl.segments:
            su = sg.uri if sg.uri.startswith("http") else urljoin(bu, sg.uri)
            segs.append(su)
        
        return segs, hi

    async def download(self):
        """Main download method"""
        try:
            logger.info(f"Starting download for {self.qname}")
            
            # Parse video playlist
            vs, vhi = self._parse_pl(self.url, self.vdir)
            logger.info(f"Video: {len(vs)} segments, init: {vhi}")
            
            # Parse audio playlist if available
            aus = []
            ahi = False
            if self.audio_url and os.path.exists(self.adir):
                try:
                    aus, ahi = self._parse_pl(self.audio_url, self.adir)
                    logger.info(f"Audio: {len(aus)} segments, init: {ahi}")
                except Exception as e:
                    logger.error(f"Audio parse error: {e}")
                    aus = []
            
            # Download segments
            await self._dl_segs(vs, self.vdir, "📹 Video")
            if aus:
                await self._dl_segs(aus, self.adir, "🔊 Audio")
            
            # Merge and convert
            await self._merge(vs, aus, vhi, ahi)
            
            logger.info(f"Download complete: {self.out}")
            return self.out
            
        except Exception as e:
            logger.error(f"Download error: {e}")
            if os.path.exists(self.tmp):
                shutil.rmtree(self.tmp)
            raise

    async def _dl_segs(self, segs, sd, label=""):
        """Download segments in parallel"""
        total = len(segs)
        done = 0
        tbytes = 0
        failed = []
        t0 = time.time()
        lu = 0

        def dl1(args):
            i, url = args
            path = os.path.join(sd, f"{i:05d}.seg")
            
            for attempt in range(3):
                try:
                    r = self.session.get(url, timeout=30)
                    if r.status_code == 200:
                        with open(path, 'wb') as f:
                            f.write(r.content)
                        return i, True, len(r.content)
                except Exception as e:
                    logger.debug(f"Segment {i} attempt {attempt+1} failed: {e}")
                    time.sleep(0.5)
            return i, False, 0

        with ThreadPoolExecutor(max_workers=20) as ex:
            futs = {ex.submit(dl1, (i, u)): i for i, u in enumerate(segs)}
            
            for fut in as_completed(futs):
                if is_cancelled(self.cid):
                    ex.shutdown(wait=False, cancel_futures=True)
                    raise Exception("CANCELLED_BY_USER")
                
                if time.time() - t0 > TASK_TIMEOUT:
                    ex.shutdown(wait=False, cancel_futures=True)
                    raise Exception("TIMEOUT: 60 minutes exceeded")
                
                i, ok, sz = fut.result()
                done += 1
                if ok:
                    tbytes += sz
                else:
                    failed.append(i)
                
                # Update progress
                now = time.time()
                if now - lu >= 1.5 or done == total:
                    el = now - t0
                    sp = tbytes / el if el > 0 else 0
                    pct = (done / total) * 100
                    eta = ((total - done) / done) * el if done > 0 else 0
                    tl = max(0, TASK_TIMEOUT - el)
                    
                    msg = (
                        f"📥 **{label} [{self.qname}]**\n"
                        f"{'─'*30}\n\n"
                        f"`{progress_bar(pct)}` **{pct:.1f}%**\n\n"
                        f"📊 {done}/{total} | 💾 {format_bytes(tbytes)}\n"
                        f"⚡ {format_bytes(sp)}/s | ETA: {format_time(eta)}\n"
                        f"⏰ Timeout: {format_time(tl)}"
                    )
                    await self.cb(msg)
                    lu = now
        
        # Retry failed segments
        if failed:
            logger.warning(f"Retrying {len(failed)} failed segments")
            for i in failed:
                try:
                    dl1((i, segs[i]))
                except:
                    pass

    def _concat(self, sd, n, hi, op):
        """Concatenate segments"""
        with open(op, 'wb') as of:
            if hi:
                ip = os.path.join(sd, "init.mp4")
                if os.path.exists(ip):
                    with open(ip, 'rb') as f:
                        of.write(f.read())
            
            for i in range(n):
                sp = os.path.join(sd, f"{i:05d}.seg")
                if os.path.exists(sp):
                    with open(sp, 'rb') as f:
                        of.write(f.read())

    async def _merge(self, vs, aus, vhi, ahi):
        """Merge video and audio streams"""
        if is_cancelled(self.cid):
            raise Exception("CANCELLED_BY_USER")
        
        await self.cb(f"🔗 **Merging Video [{self.qname}]...**")
        
        # Concatenate video
        vc = os.path.join(self.tmp, "vc.mp4")
        self._concat(self.vdir, len(vs), vhi, vc)
        
        # Concatenate audio if available
        ac = None
        if aus and os.path.exists(self.adir):
            await self.cb(f"🔗 **Merging Audio [{self.qname}]...**")
            ac = os.path.join(self.tmp, "ac.mp4")
            self._concat(self.adir, len(aus), ahi, ac)
        
        await self.cb(f"🎬 **Converting [{self.qname}]...**")
        
        # Convert to final format
        ok = False
        if ac and os.path.exists(ac) and os.path.getsize(ac) > 500:
            # Try with audio encoding first
            subprocess.run([
                'ffmpeg', '-y', '-i', vc, '-i', ac,
                '-c:v', 'copy', '-c:a', 'aac',
                '-map', '0:v:0', '-map', '1:a:0',
                '-movflags', '+faststart', '-shortest', self.out
            ], capture_output=True, timeout=600)
            
            if os.path.exists(self.out) and os.path.getsize(self.out) > 10000:
                ok = True
            else:
                # Fallback to stream copy
                subprocess.run([
                    'ffmpeg', '-y', '-i', vc, '-i', ac,
                    '-c', 'copy', '-map', '0:v:0', '-map', '1:a:0',
                    '-movflags', '+faststart', '-shortest', self.out
                ], capture_output=True, timeout=600)
                if os.path.exists(self.out) and os.path.getsize(self.out) > 10000:
                    ok = True
        
        if not ok:
            # Video only
            subprocess.run([
                'ffmpeg', '-y', '-i', vc, '-c', 'copy',
                '-map', '0', '-movflags', '+faststart', self.out
            ], capture_output=True, timeout=600)
            
            if not (os.path.exists(self.out) and os.path.getsize(self.out) > 10000):
                shutil.copy(vc, self.out)
        
        # Cleanup temp
        if os.path.exists(self.tmp):
            shutil.rmtree(self.tmp, ignore_errors=True)

# ============================================
# TELEGRAM COMMAND HANDLERS
# ============================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    st = await task_queue.get_status()
    
    welcome_msg = (
        "╔═══════════════════════════════╗\n"
        "║  🤖 **M3U8 VIDEO DOWNLOADER**   ║\n"
        "╚═══════════════════════════════╝\n\n"
        "📖 **Commands:**\n"
        "├ `/m3u8 <urls>` - Download (multi URL)\n"
        "├ `/allcancel` - Cancel all tasks\n"
        "├ `/status` - Queue status\n"
        "└ `/help` - Help\n\n"
        "✨ **Features:**\n"
        "├ 📺 Multiple URLs at once\n"
        "├ 🔊 Audio language selection\n"
        "├ ⚡ 15 parallel users\n"
        "├ ⏰ 60min auto-timeout\n"
        "├ ❌ Cancel anytime\n"
        "├ 🗑️ Auto-cleanup files\n"
        "├ 🔪 Auto-split large files\n"
        "└ 🎬 With thumbnails\n\n"
        f"📊 Active: {st['active']}/{st['max']} | Queue: {st['queue']}"
    )
    
    await update.message.reply_text(welcome_msg, parse_mode='Markdown')
    
    # Update stats
    global bot_status
    bot_status['total_downloads'] += 1

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command"""
    st = await task_queue.get_status()
    cid = update.effective_chat.id
    pos = await task_queue.get_position(cid)
    
    msg = f"📊 **STATUS**\n{'─'*30}\n\n"
    msg += f"🔄 Active: **{st['active']}/{st['max']}**\n"
    msg += f"📋 Queue: **{st['queue']}/{MAX_QUEUE}**\n\n"
    
    if pos == 0:
        msg += "✅ Your download is active!"
    elif pos > 0:
        msg += f"⏳ Your position: **{pos}**"
    else:
        msg += "💤 No active downloads"
    
    await update.message.reply_text(msg, parse_mode='Markdown')

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    help_msg = (
        "📖 **HELP**\n\n"
        "**Single URL:**\n"
        "`/m3u8 https://example.com/video.m3u8`\n\n"
        "**Multiple URLs:**\n"
        "`/m3u8 url1 url2 url3`\n\n"
        "**Steps:**\n"
        "1️⃣ Send URL(s)\n"
        "2️⃣ Select quality (once for all)\n"
        "3️⃣ Select audio language\n"
        "4️⃣ All videos download automatically!\n\n"
        "**Cancel:**\n"
        "├ ⏭ Skip = skip current video\n"
        "├ ❌ Cancel All = stop everything\n"
        "└ `/allcancel` = cancel all\n\n"
        "🗑️ Files auto-delete after upload/cancel\n"
        f"⏰ Timeout: 60 min per video\n\n"
        f"**Server Stats:**\n"
        f"├ Uptime: {format_time(time.time() - bot_start_time)}\n"
        f"└ Memory: {bot_status['memory_usage']:.1f} MB"
    )
    
    await update.message.reply_text(help_msg, parse_mode='Markdown')

async def allcancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /allcancel command"""
    cid = update.effective_chat.id
    cancel_flags[cid] = 'all'
    await task_queue.remove_all(cid)
    cleanup_user_temp(cid)
    cleanup_user_downloads(cid)
    
    await update.message.reply_text(
        "🚫 **All tasks cancelled!**\n🗑️ All files cleaned up.",
        parse_mode='Markdown'
    )
    
    # Update stats
    global bot_status
    bot_status['total_errors'] += 1

async def m3u8_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /m3u8 command"""
    cid = update.effective_chat.id
    full = update.message.text
    
    # Extract URLs from command
    parts = full.split()
    if len(parts) < 2:
        await update.message.reply_text(
            "❌ **No URLs!**\n\n"
            "**Single:** `/m3u8 <url>`\n"
            "**Multi:** `/m3u8 url1 url2 url3`",
            parse_mode='Markdown'
        )
        return
    
    urls = []
    for part in parts[1:]:
        if part.startswith(('http://', 'https://')):
            urls.append(part)
    
    if not urls:
        await update.message.reply_text(
            "❌ **No valid URLs found!**",
            parse_mode='Markdown'
        )
        return

    ui = f"📦 **{len(urls)} URL(s)**" if len(urls) > 1 else "📦 **1 URL**"
    msg = await update.message.reply_text(
        f"🔍 **Analyzing...**\n{ui}",
        parse_mode='Markdown'
    )

    # Parse first URL to get qualities
    result = await parse_m3u8(urls[0])
    if not result:
        await msg.edit_text("❌ **Failed to parse M3U8**")
        return

    qs = result['qualities']
    ats = result['audio_tracks']
    
    if not qs:
        await msg.edit_text("❌ **No qualities found**")
        return

    # Store user data
    user_data_store[cid] = {
        'urls': urls,
        'qualities': qs,
        'audio_tracks': ats,
        'sel_h': None,
        'sel_al': '',
        'sel_an': '',
        'is_all': False,
    }

    # Build quality selection keyboard
    kb = []
    if len(qs) > 1:
        kb.append([InlineKeyboardButton(
            f"📥 ALL QUALITIES ({len(qs)})",
            callback_data="quality_all"
        )])
        kb.append([InlineKeyboardButton("─────────────────────", callback_data="sep")])
    
    for i, q in enumerate(qs):
        if q['name'] != 'default':
            btn = f"📺 {q['name']} • {q['resolution']} • {q['bandwidth']//1000}kbps"
        else:
            btn = "📺 Default"
        kb.append([InlineKeyboardButton(btn, callback_data=f"quality_{i}")])
    
    kb.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])

    # Audio info
    ai = ""
    if len(ats) > 1:
        ai = f"🔊 **{len(ats)} languages:** {', '.join(t['display'] for t in ats[:3])}\n"
        if len(ats) > 3:
            ai += f"   ... and {len(ats)-3} more\n"
        ai += "👉 Choose after quality!\n\n"
    elif ats:
        ai = f"🔊 Audio: {ats[0]['display']}\n\n"
    else:
        ai = "🔊 Audio: Muxed\n\n"

    # URLs list
    ul = ""
    if len(urls) > 1:
        ul = "📋 **URLs:**\n"
        for i, u in enumerate(urls[:5]):
            ul += f"   {i+1}. `{u[:50]}...`\n"
        if len(urls) > 5:
            ul += f"   ... +{len(urls)-5} more\n"
        ul += "\n"

    st = await task_queue.get_status()
    
    await msg.edit_text(
        f"🎬 **STEP 1: SELECT QUALITY**\n\n{ui}\n{ul}{ai}"
        f"📊 Server: {st['active']}/{st['max']} active",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode='Markdown'
    )

# ============================================
# CALLBACK HANDLER
# ============================================
async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries"""
    q = update.callback_query
    await q.answer()
    
    cid = update.effective_chat.id
    data = q.data

    if data == "sep":
        return

    if data == "cancel_current":
        cancel_flags[cid] = 'current'
        try:
            await q.edit_message_text("⏭ **Skipping current...**", parse_mode='Markdown')
        except:
            pass
        return

    if data == "cancel_all_batch":
        cancel_flags[cid] = 'all'
        cleanup_user_temp(cid)
        try:
            await q.edit_message_text(
                "🚫 **Cancelling all...**\n🗑️ Cleaning files...",
                parse_mode='Markdown'
            )
        except:
            pass
        return

    if data == "cancel":
        await q.edit_message_text("❌ **Cancelled**", parse_mode='Markdown')
        user_data_store.pop(cid, None)
        return

    ud = user_data_store.get(cid)
    if not ud:
        await q.edit_message_text("❌ **Expired. Send /m3u8 again.**")
        return

    if data.startswith("quality_"):
        if data == "quality_all":
            ud['is_all'] = True
            ud['sel_h'] = None
        else:
            try:
                qi = int(data.split('_')[1])
                ud['is_all'] = False
                ud['sel_h'] = ud['qualities'][qi]['height']
            except (ValueError, IndexError):
                await q.edit_message_text("❌ **Invalid quality selection**")
                return

        ats = ud.get('audio_tracks', [])
        
        if len(ats) > 1:
            # Show audio selection
            kb = []
            for i, t in enumerate(ats):
                pf = "⭐" if is_hindi_track(t) else "🔊"
                bt = f"{pf} {t['display']}"
                if t.get('name'):
                    bt += f" ({t['name']})"
                kb.append([InlineKeyboardButton(bt, callback_data=f"audio_{i}")])
            
            kb.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
            
            qt = "ALL QUALITIES" if ud['is_all'] else f"{ud['sel_h']}p"
            n = len(ud['urls'])
            un = f"\n📦 Applies to **{n} URL(s)**" if n > 1 else ""
            
            await q.edit_message_text(
                f"🔊 **STEP 2: SELECT AUDIO**\n\n"
                f"📺 Video: **{qt}**{un}\n\n"
                f"👇 Choose language:\n⭐ = Hindi\n\n"
                f"Available:\n" +
                "\n".join(f"   • {t['display']}" for t in ats[:5]),
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode='Markdown'
            )
            return
        else:
            # No audio selection needed
            if ats:
                ud['sel_al'] = ats[0].get('language', '')
                ud['sel_an'] = ats[0].get('name', '')
            await _go(cid, ud, q, context)

    elif data.startswith("audio_"):
        try:
            ai = int(data.split('_')[1])
            ats = ud.get('audio_tracks', [])
            
            if ai >= len(ats):
                await q.edit_message_text("❌ **Invalid**")
                return
            
            sa = ats[ai]
            ud['sel_al'] = sa.get('language', '')
            ud['sel_an'] = sa.get('name', '')
            logger.info(f"Selected audio: {sa['display']}")
            
            await _go(cid, ud, q, context)
        except (ValueError, IndexError):
            await q.edit_message_text("❌ **Invalid audio selection**")

async def _go(cid, ud, query, context):
    """Proceed with download after selections"""
    qt = "ALL" if ud['is_all'] else f"{ud['sel_h']}p"
    ad = get_lang_display(ud['sel_al'], ud['sel_an']) if ud['sel_al'] else "Muxed"
    n = len(ud['urls'])
    
    await query.edit_message_text(
        f"✅ **Quality: {qt}** | 🔊 **{ad}** | 📦 **{n} URL(s)**\n\n⏳ Starting...",
        parse_mode='Markdown'
    )

    ti = {
        'urls': ud['urls'],
        'is_all': ud['is_all'],
        'sel_h': ud['sel_h'],
        'sel_al': ud.get('sel_al', ''),
        'sel_an': ud.get('sel_an', ''),
        'context': context
    }

    can, qm = await task_queue.add_task(cid, ti)
    
    if not can:
        await context.bot.send_message(cid, qm, parse_mode='Markdown')
        return

    asyncio.create_task(process_batch(cid, ti, context))
    
    if "queue" in qm.lower():
        await context.bot.send_message(cid, qm, parse_mode='Markdown')

# ============================================
# BATCH PROCESSING
# ============================================
async def process_batch(cid, ti, ctx):
    """Process batch of URLs"""
    global bot_status
    
    cancel_flags[cid] = 'none'
    urls = ti['urls']
    nu = len(urls)
    is_all_q = ti['is_all']
    th = ti['sel_h']
    tal = ti.get('sel_al', '')
    tan = ti.get('sel_an', '')

    pm = None
    t0 = time.time()
    uploaded = 0
    total_sz = 0
    failed = 0
    skipped = 0

    def ckb():
        if nu > 1:
            return InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Skip", callback_data="cancel_current"),
                 InlineKeyboardButton("❌ Cancel All", callback_data="cancel_all_batch")]
            ])
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_current")]
        ])

    async def up(text):
        nonlocal pm
        try:
            kb = ckb()
            if pm is None:
                pm = await ctx.bot.send_message(
                    cid, text,
                    parse_mode='Markdown',
                    reply_markup=kb
                )
            else:
                await pm.edit_text(
                    text,
                    parse_mode='Markdown',
                    reply_markup=kb
                )
        except Exception as e:
            logger.error(f"Update error: {e}")

    try:
        # Update active downloads count
        bot_status['active_downloads'] += 1
        bot_status['queue_size'] = (await task_queue.get_status())['queue']

        for ui, url in enumerate(urls):
            if is_all_cancelled(cid):
                skipped += (nu - ui)
                break

            if cancel_flags.get(cid) == 'current':
                cancel_flags[cid] = 'none'
                skipped += 1
                continue

            ul = f"[{ui+1}/{nu}]" if nu > 1 else ""
            vn = get_video_name(url, ui, nu)

            await up(
                f"🔍 **Parsing {ul}**\n"
                f"📁 {vn}\n"
                f"🔗 `{url[:60]}...`\n\n"
                f"⏳ Analyzing..."
            )

            result = await parse_m3u8(url)
            if not result or not result['qualities']:
                await up(f"⚠️ **Skip {ul}** - Parse failed")
                failed += 1
                await asyncio.sleep(2)
                continue

            qs = result['qualities']
            ats = result['audio_tracks']

            # Select qualities
            if is_all_q:
                sqs = qs
            else:
                m = find_quality(qs, th)
                sqs = [m] if m else [qs[-1]]

            # Select audio
            ma = find_audio(ats, tal, tan)
            for q in sqs:
                q['audio_url'] = ma['url'] if ma else None

            # Download each quality
            for qi, q in enumerate(sqs):
                if is_cancelled(cid):
                    break
                
                ql = f"[Q{qi+1}/{len(sqs)}]" if len(sqs) > 1 else ""
                an = "🔊" if q.get('audio_url') else "🔇"

                await up(
                    f"📥 **DOWNLOADING {ul} {ql}**\n"
                    f"📁 {vn}\n"
                    f"📺 {q['name']} | 📐 {q['resolution']} | {an}\n\n"
                    f"⏳ Starting..."
                )

                dl = Downloader(
                    url=q['url'],
                    qname=q['name'],
                    cid=cid,
                    callback=up,
                    audio_url=q.get('audio_url'),
                    vname=vn
                )

                try:
                    ds = time.time()
                    out = await dl.download()
                    dt = time.time() - ds
                    fs = os.path.getsize(out)

                    if is_cancelled(cid):
                        cleanup_file(out)
                        cleanup_user_temp(cid)
                        break

                    await upload_video(cid, out, q, pm, dt, f"{ul} {ql}", ckb(), vn)
                    uploaded += 1
                    total_sz += fs

                    # Update stats
                    bot_status['total_uploads'] += 1

                    # Delete after upload
                    cleanup_file(out)

                except Exception as e:
                    es = str(e)
                    if "CANCELLED" in es:
                        skipped += 1
                        cleanup_user_temp(cid)
                    elif "TIMEOUT" in es:
                        failed += 1
                        await up(f"⏰ **TIMEOUT {ul}** - {vn} > 60 min")
                        cleanup_user_temp(cid)
                        await asyncio.sleep(2)
                    else:
                        failed += 1
                        await up(f"❌ **Error {ul}**\n`{es[:150]}`")
                        cleanup_user_temp(cid)
                        await asyncio.sleep(2)
                    
                    bot_status['total_errors'] += 1

            if is_all_cancelled(cid):
                skipped += max(0, nu - ui - 1)
                break
            
            if ui < nu - 1:
                await asyncio.sleep(1)

        tt = time.time() - t0
        was_c = is_all_cancelled(cid)

        # Final cleanup
        cleanup_user_temp(cid)

        summary = (
            f"{'🚫 **CANCELLED**' if was_c else '🎉 **COMPLETE!**'}\n"
            f"{'═'*30}\n\n"
            f"📦 URLs: **{nu}**\n"
            f"✅ Uploaded: **{uploaded}**\n"
            f"❌ Failed: **{failed}**\n"
            f"⏭ Skipped: **{skipped}**\n"
            f"💾 Size: **{format_bytes(total_sz)}**\n"
            f"⏱️ Time: **{format_time(tt)}**\n\n"
            f"🗑️ All temp files cleaned"
        )
        
        await ctx.bot.send_message(cid, summary, parse_mode='Markdown')
        
        try:
            if pm:
                await pm.delete()
        except:
            pass

    except Exception as e:
        logger.error(f"Batch processing error: {e}")
        cleanup_user_temp(cid)
        try:
            await ctx.bot.send_message(
                cid,
                f"❌ **Fatal Error**\n`{str(e)[:200]}`\n🗑️ Files cleaned",
                parse_mode='Markdown'
            )
        except:
            pass

    finally:
        # Cleanup
        cancel_flags.pop(cid, None)
        user_data_store.pop(cid, None)
        cleanup_user_temp(cid)
        
        # Update active downloads count
        bot_status['active_downloads'] -= 1
        bot_status['queue_size'] = (await task_queue.get_status())['queue']
        
        # Process next in queue
        nc, nt = await task_queue.complete_task(cid)
        if nc and nt:
            try:
                await ctx.bot.send_message(
                    nc,
                    "🎉 **Your turn! Download starting...**",
                    parse_mode='Markdown'
                )
            except:
                pass
            asyncio.create_task(process_batch(nc, nt, nt['context']))

# ============================================
# UPLOAD FUNCTIONS
# ============================================
async def upload_video(cid, fp, qi, pm, dt, qn="", ckb=None, vn="video"):
    """Upload video to Telegram"""
    global pyro_client
    
    fs = os.path.getsize(fp)
    tp = os.path.join(DOWNLOAD_FOLDER, f"thumb_{cid}.jpg")
    th = make_thumb(fp, tp)
    w, h, d = get_meta(fp)

    if fs > MAX_FILE_SIZE:
        # Split large file
        parts = split_video(fp)
        
        for i, pp in enumerate(parts):
            if is_cancelled(cid):
                break
            
            ps = os.path.getsize(pp)
            pw, ph, pd = get_meta(pp)
            ptp = os.path.join(DOWNLOAD_FOLDER, f"thumb_{cid}_p{i}.jpg")
            pth = make_thumb(pp, ptp)
            
            cap = (
                f"📹 **{vn}** {qn}\n"
                f"📺 {qi['name']} Part {i+1}/{len(parts)}\n"
                f"📐 {qi['resolution']} | 📦 {format_bytes(ps)} | ⏱️ {format_time(pd)}"
            )
            
            await _upload(cid, pp, cap, pw, ph, pd, pth, pm, qi['name'], ckb)
            
            # Cleanup part
            os.remove(pp)
            if pth and os.path.exists(pth):
                os.remove(pth)
    else:
        # Upload single file
        cap = (
            f"✅ **{vn}** {qn}\n"
            f"📺 {qi['name']} | 📐 {qi['resolution']}\n"
            f"📦 {format_bytes(fs)} | ⏱️ {format_time(d)}\n"
            f"📊 {qi['bandwidth']//1000} kbps | ⏱️ DL: {format_time(dt)}"
        )
        await _upload(cid, fp, cap, w, h, d, th, pm, qi['name'], ckb)
    
    # Cleanup thumbnail
    if th and os.path.exists(th):
        os.remove(th)

async def _upload(cid, fp, cap, w, h, d, th, pm, qn, ckb=None):
    """Internal upload function with progress"""
    global pyro_client
    
    t0 = time.time()
    lu = [time.time()]

    async def pc(cur, tot):
        now = time.time()
        if now - lu[0] >= 2 or cur == tot:
            el = now - t0
            sp = cur / el if el > 0 else 0
            pct = (cur / tot) * 100
            eta = (tot - cur) / sp if sp > 0 else 0
            
            txt = (
                f"📤 **UPLOADING [{qn}]**\n"
                f"{'─'*30}\n\n"
                f"`{progress_bar(pct)}` **{pct:.1f}%**\n\n"
                f"📊 {format_bytes(cur)}/{format_bytes(tot)}\n"
                f"⚡ {format_bytes(sp)}/s | ETA: {format_time(eta)}"
            )
            
            try:
                await pm.edit_text(txt, parse_mode='Markdown', reply_markup=ckb)
            except Exception as e:
                logger.debug(f"Upload progress update error: {e}")
            
            lu[0] = now

    try:
        if not pyro_client.is_connected:
            await pyro_client.start()
        
        # Try sending as video
        await pyro_client.send_video(
            chat_id=cid,
            video=fp,
            caption=cap,
            duration=d if d > 0 else None,
            width=w if w > 0 else None,
            height=h if h > 0 else None,
            thumb=th,
            supports_streaming=True,
            progress=pc
        )
        logger.info(f"Uploaded video: {fp}")
        
    except Exception as e:
        logger.error(f"Video upload failed: {e}")
        
        try:
            # Fallback to document
            await pyro_client.send_document(
                chat_id=cid,
                document=fp,
                caption=cap,
                thumb=th,
                progress=pc
            )
            logger.info(f"Uploaded as document: {fp}")
            
        except Exception as e2:
            logger.error(f"Document upload failed: {e2}")
            try:
                await pm.edit_text(
                    f"❌ **Upload Failed**\n`{str(e2)[:200]}`",
                    parse_mode='Markdown'
                )
            except:
                pass

# ============================================
# SIGNAL HANDLERS
# ============================================
def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {sig}, shutting down...")
    
    # Cleanup all temp files
    try:
        if os.path.exists(TEMP_FOLDER):
            shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
        if os.path.exists(DOWNLOAD_FOLDER):
            # Don't delete downloads folder on shutdown
            pass
    except Exception as e:
        logger.error(f"Cleanup error on shutdown: {e}")
    
    # Stop pyrogram client
    if pyro_client and pyro_client.is_connected:
        asyncio.create_task(pyro_client.stop())
    
    sys.exit(0)

# ============================================
# MAIN FUNCTION
# ============================================
async def main():
    """Main function"""
    global pyro_client, bot_status
    
    logger.info("╔════════════════════════════════════════════╗")
    logger.info("║  🤖 M3U8 BOT - OPTIMIZED FOR RENDER       ║")
    logger.info("╚════════════════════════════════════════════╝")
    logger.info(f"   📂 Base: {BASE_DIR}")
    logger.info(f"   📥 Downloads: {DOWNLOAD_FOLDER}")
    logger.info(f"   📁 Temp: {TEMP_FOLDER}")
    logger.info(f"   ⚙️ Workers: {MAX_WORKERS} | Queue: {MAX_QUEUE}")
    logger.info(f"   ⏰ Timeout: {TASK_TIMEOUT//60}min")
    logger.info(f"   📦 Max: {format_bytes(MAX_FILE_SIZE)}")
    logger.info(f"   🌐 Web port: {PORT}")

    # Clean old session files
    for f in os.listdir(BASE_DIR):
        if f.endswith('.session') or f.endswith('.session-journal'):
            try:
                os.remove(os.path.join(BASE_DIR, f))
                logger.info(f"Removed old session: {f}")
            except:
                pass

    # Clean temp folder on start
    if os.path.exists(TEMP_FOLDER):
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
    
    # Create fresh directories
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    os.makedirs(LOG_FOLDER, exist_ok=True)

    # Start periodic cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    logger.info("   ✅ Periodic cleanup started")

    # Start Pyrogram client
    pyro_client = Client(
        name="m3u8bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True
    )
    
    await pyro_client.start()
    logger.info("   ✅ Pyrogram client connected")

    # Update status
    bot_status['status'] = 'running'

    # Setup Telegram bot
    telegram_app = Application.builder().token(BOT_TOKEN).build()
    
    # Add command handlers
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("status", status_cmd))
    telegram_app.add_handler(CommandHandler("help", help_cmd))
    telegram_app.add_handler(CommandHandler("m3u8", m3u8_cmd))
    telegram_app.add_handler(CommandHandler("allcancel", allcancel_cmd))
    telegram_app.add_handler(CallbackQueryHandler(cb_handler))

    logger.info("   ✅ Telegram bot configured")
    logger.info("╔════════════════════════════════════════════╗")
    logger.info("║        🎉  BOT IS RUNNING!  🎉             ║")
    logger.info("╚════════════════════════════════════════════╝")
    logger.info(f"   📱 Send /start to @{(await telegram_app.bot.get_me()).username}")
    logger.info(f"   🌐 Health check: http://localhost:{PORT}/health")
    logger.info(f"   🛑 Ctrl+C to stop\n")

    # Run bot with polling
    await telegram_app.run_polling(drop_pending_updates=True)

# ============================================
# ENTRY POINT
# ============================================
if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Run main bot
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("\n   🛑 Stopped by user")
    except Exception as e:
        logger.error(f"\n   ❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        if os.path.exists(TEMP_FOLDER):
            shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
        logger.info("   👋 Goodbye!")
