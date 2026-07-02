# ─────────────────────────────────────────────────────────────────────────────
# Movie-Stream Backend — FastAPI Server
# ─────────────────────────────────────────────────────────────────────────────
# Author  : ThiruXD
# GitHub  : https://github.com/ThiruXD
# Portfolio: https://thiruxd.is-a.dev
# ─────────────────────────────────────────────────────────────────────────────

import asyncio
import orjson          # Ultra-fast binary JSON serialiser – used for NDJSON streaming
import ujson           # Fast JSON for standard API responses (2-3x faster than stdlib json)
import ijson           # Incremental/streaming JSON parser – used for large restore uploads
from datetime import datetime, timedelta
from time import time
from typing import Any, Dict, List, Optional, Union
from Backend.helper.encrypt import decode_string
from fastapi.responses import StreamingResponse, HTMLResponse, ORJSONResponse
from fastapi import FastAPI, Query, Request, HTTPException, BackgroundTasks, Depends
import urllib.parse
from urllib.parse import quote
from fastapi.templating import Jinja2Templates
import jwt
from passlib.context import CryptContext
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

import mimetypes
import secrets
import math

from Backend.logger import LOGGER
from Backend.config import Telegram
from Backend.pyrofork import StreamBot, work_loads, multi_clients
from Backend.helper.exceptions import InvalidHash, FIleNotFound
from Backend.helper.custom_dl import ByteStreamer
from fastapi.middleware.cors import CORSMiddleware
from Backend.helper.pyro import get_readable_time, normalize_languages
from Backend.helper.modal import SettingsSchema, MovieSchema, TVShowSchema, MovieListSchema, TVShowListSchema, HomeSectionSchema
from Backend import StartTime, __version__, db
from Backend.helper.metadata import search_tmdb, search_imdb, get_external_details, fetch_movie_metadata, metadata as get_media_metadata, tmdb
from Backend.helper.notification import send_log_report, send_bulk_log_report
import PTN
from bson import ObjectId
from pymongo import ASCENDING, DESCENDING
from collections import OrderedDict


# ─────────────────────────────────────────────────────────────────────────────
# LRU In-Memory Cache with tiered TTL, hit tracking & efficient eviction
# Author: ThiruXD | https://thiruxd.is-a.dev
# ─────────────────────────────────────────────────────────────────────────────
class LRUCache:
    """Thread-safe LRU cache with per-key TTL and automatic eviction.
    
    Improvements over SimpleCache:
      • OrderedDict-based LRU — most-recently-used items survive eviction
      • Lazy expiry on read + periodic cleanup on write (no stale data served)
      • Hit/miss counters for monitoring
      • Per-key TTL tiers — hot endpoints (settings, home) get longer TTLs
    """

    def __init__(self, default_ttl: int = 120, maxsize: int = 2000):
        self._store: OrderedDict = OrderedDict()
        self.default_ttl = default_ttl
        self.maxsize = maxsize
        self.hits = 0
        self.misses = 0
        self._cleanup_counter = 0

    # ── read ──────────────────────────────────────────────────────────────
    def get(self, key: str):
        entry = self._store.get(key)
        if entry is None:
            self.misses += 1
            return None

        if time() - entry["time"] >= entry["ttl"]:
            # Expired — evict lazily
            del self._store[key]
            self.misses += 1
            return None

        # Move to end (most recently used)
        self._store.move_to_end(key)
        self.hits += 1
        return entry["value"]

    # ── write ─────────────────────────────────────────────────────────────
    def set(self, key: str, value, expire: int = None):
        ttl = expire if expire is not None else self.default_ttl

        if key in self._store:
            # Update in-place & move to end
            self._store[key] = {"value": value, "time": time(), "ttl": ttl}
            self._store.move_to_end(key)
        else:
            self._store[key] = {"value": value, "time": time(), "ttl": ttl}

        # Periodic cleanup every 50 writes
        self._cleanup_counter += 1
        if self._cleanup_counter >= 50:
            self._cleanup()
            self._cleanup_counter = 0

        # LRU eviction if over capacity
        while len(self._store) > self.maxsize:
            self._store.popitem(last=False)  # Remove oldest (least recently used)

    # ── delete / clear ────────────────────────────────────────────────────
    def delete(self, key: str):
        self._store.pop(key, None)

    def clear(self):
        self._store.clear()
        self.hits = 0
        self.misses = 0

    # ── internal cleanup (remove expired entries) ─────────────────────────
    def _cleanup(self):
        now = time()
        expired_keys = [k for k, v in self._store.items() if now - v["time"] >= v["ttl"]]
        for k in expired_keys:
            del self._store[k]

    # ── stats (useful for admin/debug endpoints) ──────────────────────────
    @property
    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "size": len(self._store),
            "maxsize": self.maxsize,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{(self.hits / total * 100):.1f}%" if total > 0 else "N/A",
            "default_ttl": self.default_ttl,
        }

# ── Cache TTL tiers ──────────────────────────────────────────────────────────
# Endpoint               TTL (seconds)   Reason
# settings               600             Rarely changes, hot path
# home_data              300             Moderate churn, expensive query
# sort_movies / sort_tv  120             Default — paginated catalog
# search                 60              User-specific, short-lived
# ─────────────────────────────────────────────────────────────────────────────
api_cache = LRUCache(default_ttl=120, maxsize=2000)

# Use ORJSONResponse as the global default – all standard JSON endpoints
# benefit from orjson's ~3x serialisation speedup over stdlib json.
app = FastAPI(default_response_class=ORJSONResponse)
class_cache = {}

templates = Jinja2Templates(directory="Backend/fastapi/templates")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()
JWT_SECRET = Telegram.API_HASH # Use API_HASH as a secret if not otherwise specified
JWT_ALGORITHM = "HS256"

async def verify_admin(auth: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(auth.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Invalid role")
        return payload
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

async def get_admin_user(username: str):
    admin = await db.get_admin_credentials()
    if admin and admin["username"] == username:
        return admin
    return None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


@app.get("/api/ping")
async def ping():
    return {"ping": "pong", "version": __version__, "db": str(db.db_name)}

@app.get("/", response_model=Dict[str, Any])
async def get_bot_workloads():
    """
    Home route to list each bot's workload and total number of bots.
    """
    response = {
            "server_status": "running",
            "uptime": get_readable_time(time() - StartTime),
            "telegram_bot": "@" + StreamBot.username,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": __version__,
        }
    return response



@app.get("/is_member")
async def is_member(user_id: int, channel: int):
    try:
        member = await StreamBot.get_chat_member(channel, user_id)
        if member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            return {"is_member": True}
        else:
            return {"is_member": False}
    except Exception as e:
        return {"is_member": False}


@app.get("/watch/{tmdb_id}", response_class=HTMLResponse)
async def watch(
    request: Request, 
    tmdb_id: int, 
    season_number: Optional[int] = Query(None), 
    episode_number: Optional[int] = Query(None)
):
    """
    Serve the appropriate HTML template for watching a movie or a specific TV episode.

    :param request: The incoming HTTP request.
    :param tmdb_id: The TMDB ID of the movie or TV show.
    :param season_number: The season number (optional, only for TV shows).
    :param episode_number: The episode number (optional, only for TV shows).
    :return: The rendered HTML template.
    """

    return templates.TemplateResponse(
        "index.html", 
        {
            "request": request, 
            "id": tmdb_id, 
            "season": season_number, 
            "episode": episode_number
        }
    )



@app.get("/api/tvshows", response_model=dict)
async def get_sorted_tv_shows(
    sort_by: List[str] = Query(default=["updated_on:desc"], description="List of fields to sort by. Format: field:direction"),
    page: int = Query(default=1, ge=1, description="Page number to return"),
    page_size: int = Query(default=10, ge=1, description="Number of TV shows per page"),
    genre: Optional[str] = Query(default=None, description="Optional genre to filter by"),
    year: Optional[str] = Query(default=None, description="Optional release year to filter by"),
    audio: Optional[str] = Query(default=None, description="Optional audio language to filter by"),
    query: Optional[str] = Query(default=None, description="Optional text search query")
):
    cache_key = f"tvs_{sort_by}_{page}_{page_size}_{genre}_{year}_{audio}_{query}"
    cached = api_cache.get(cache_key)
    if cached:
        return cached
    try:
        sort_params = [tuple(param.split(":")) for param in sort_by]
        sorted_tv_shows = await db.sort_tv_shows(sort_params, page, page_size, genre, year, audio, query)
        api_cache.set(cache_key, sorted_tv_shows)
        return sorted_tv_shows
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/tvs", response_model=dict)
async def get_sorted_tvs(
    sort_by: List[str] = Query(default=["updated_on:desc"]),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1),
    genre: Optional[str] = Query(default=None),
    year: Optional[str] = Query(default=None),
    audio: Optional[str] = Query(default=None),
    query: Optional[str] = Query(default=None)
):
    return await get_sorted_tv_shows(sort_by, page, page_size, genre, year, audio, query)

@app.get("/api/movies", response_model=dict)
async def get_sorted_movies(
    sort_by: List[str] = Query(default=["updated_on:desc"], description="List of fields to sort by. Format: field:direction"),
    page: int = Query(default=1, ge=1, description="Page number to return"),
    page_size: int = Query(default=10, ge=1, description="Number of movies per page"),
    genre: Optional[str] = Query(default=None, description="Optional genre to filter by"),
    year: Optional[str] = Query(default=None, description="Optional release year to filter by"),
    audio: Optional[str] = Query(default=None, description="Optional audio language to filter by"),
    query: Optional[str] = Query(default=None, description="Optional text search query")
):
    cache_key = f"movies_{sort_by}_{page}_{page_size}_{genre}_{year}_{audio}_{query}"
    cached = api_cache.get(cache_key)
    if cached:
        return cached
    try:
        sort_params = [tuple(param.split(":")) for param in sort_by]
        sorted_movies = await db.sort_movies(sort_params, page, page_size, genre, year, audio, query)
        api_cache.set(cache_key, sorted_movies)
        return sorted_movies
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/trending", response_model=dict)
async def get_trending(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1)
):
    cache_key = f"trending_{page}_{page_size}"
    cached = api_cache.get(cache_key)
    if cached:
        return cached
    try:
        result = await db.get_trending_media(page, page_size)
        api_cache.set(cache_key, result)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/home")
async def get_home_data():
    """Consolidated endpoint for Home page to reduce network requests."""
    cache_key = "home_data"
    cached = api_cache.get(cache_key)
    if cached:
        return cached
    try:
        hero, sections = await asyncio.gather(
            db.get_trending_media(1, 10),
            db.get_populated_home_sections()
        )
        
        # Backward compatibility fallbacks
        latest_movies = []
        latest_tv = []
        trending_now = []
        for sec in sections:
            if sec.get("section_type") == "latest":
                if sec.get("media_type") == "movie":
                    latest_movies = sec.get("items", [])
                elif sec.get("media_type") == "tv":
                    latest_tv = sec.get("items", [])
            elif sec.get("section_type") == "trending" and sec.get("media_type") == "both":
                trending_now = sec.get("items", [])

        result = {
            "hero": hero.get("results", []),
            "sections": sections,
            "latest_movies": latest_movies,
            "latest_tv": latest_tv,
            "trending_now": trending_now
        }
        api_cache.set(cache_key, result, expire=300)
        return result
    except Exception as e:
        LOGGER.error(f"Error in /api/home: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/home/stream")
async def stream_home_data():
    """NDJSON streaming endpoint for Home page.
    
    Sends data progressively so the frontend can render section-by-section:
      Line 1: {"type": "hero", "data": [...]}           — hero slider items
      Line 2: {"type": "section", "data": {...}}         — first home section
      Line 3: {"type": "section", "data": {...}}         — second home section
      ...
      Last:   {"type": "done"}                           — signals end of stream
    
    JSON stack: orjson serialiser + NDJSON framing.
    """
    async def stream_generator():
        try:
            # 1) Hero data — send immediately so the slider renders first
            hero = await db.get_trending_media(1, 10)
            # hero["results"] contains Pydantic schema objects — convert to dicts for orjson
            hero_items = [item.dict() if hasattr(item, "dict") else item for item in hero.get("results", [])]
            yield orjson.dumps({"type": "hero", "data": hero_items}) + b"\n"

            # 2) Fetch section definitions (enabled, sorted by position)
            sections = await db.home_sections.find({"enabled": True}).sort([("position", ASCENDING)]).to_list(None)
            settings = await db.get_settings()
            priority = settings.get("language_priority", [])

            # 3) Populate & stream each section individually
            for sec in sections:
                try:
                    sec = db._convert_object_id(sec)
                    sec_type = sec.get("section_type")
                    media_type = sec.get("media_type", "both")
                    limit = sec.get("limit", 10)

                    items = []
                    if sec_type == "latest":
                        if media_type == "movie":
                            res = await db.movie_collection.find().sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                            items = [db._convert_object_id(x) for x in res]
                        elif media_type == "tv":
                            res = await db.tv_collection.find().sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                            items = [db._convert_object_id(x) for x in res]
                        else:
                            movies, tvshows = await asyncio.gather(
                                db.movie_collection.find().sort([("updated_on", DESCENDING)]).limit(limit).to_list(None),
                                db.tv_collection.find().sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                            )
                            combined = [db._convert_object_id(x) for x in movies] + [db._convert_object_id(x) for x in tvshows]
                            combined.sort(key=lambda x: x.get("updated_on") if x.get("updated_on") else datetime.min, reverse=True)
                            items = combined[:limit]
                    elif sec_type == "trending":
                        if media_type == "movie":
                            res = await db.movie_collection.find().sort([("views", DESCENDING)]).limit(limit).to_list(None)
                            items = [db._convert_object_id(x) for x in res]
                        elif media_type == "tv":
                            res = await db.tv_collection.find().sort([("views", DESCENDING)]).limit(limit).to_list(None)
                            items = [db._convert_object_id(x) for x in res]
                        else:
                            movies, tvshows = await asyncio.gather(
                                db.movie_collection.find().sort([("views", DESCENDING)]).limit(limit).to_list(None),
                                db.tv_collection.find().sort([("views", DESCENDING)]).limit(limit).to_list(None)
                            )
                            combined = [db._convert_object_id(x) for x in movies] + [db._convert_object_id(x) for x in tvshows]
                            combined.sort(key=lambda x: x.get("views", 0), reverse=True)
                            items = combined[:limit]
                    elif sec_type == "top_release":
                        raw_items = sec.get("items", [])[:limit]
                        items = await db._populate_items(raw_items)
                    elif sec_type == "recently_watched":
                        items = []

                    # Sort languages by priority
                    for item in items:
                        if "languages" in item:
                            item["languages"] = db._sort_languages(item["languages"], priority)

                    sec["items"] = items
                    yield orjson.dumps({"type": "section", "data": sec}) + b"\n"
                except Exception as sec_err:
                    LOGGER.error(f"Error populating section {sec.get('title', '?')}: {sec_err}")
                    continue

            # 4) Signal completion
            yield orjson.dumps({"type": "done"}) + b"\n"
        except Exception as e:
            LOGGER.error(f"Error in /api/home/stream: {e}")
            yield orjson.dumps({"type": "error", "message": str(e)}) + b"\n"

    return StreamingResponse(
        stream_generator(),
        media_type="application/x-ndjson",
        headers={
            "X-Content-Type-Options": "nosniff",
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "Content-Encoding": "identity",
        }
    )

@app.post("/api/view/{media_type}/{tmdb_id}")
async def increment_media_view(
    request: Request, 
    media_type: str, 
    tmdb_id: Union[int, str],
    title: Optional[str] = Query(None),
    doc_id: Optional[str] = Query(None)
):
    ip = request.client.host
    user_agent = request.headers.get("user-agent", "")
    identifier = f"{ip}_{user_agent}"
    await db.increment_view(media_type, tmdb_id, identifier, title=title, doc_id=doc_id)
    return {"status": "success"}

@app.get("/api/settings", response_model=dict)
async def get_site_settings():
    try:
        settings = await db.get_settings()
        return settings
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/settings")
async def update_site_settings(settings: SettingsSchema, admin: dict = Depends(verify_admin)):
    try:
        success = await db.update_settings(settings)
        if success:
            return {"message": "Settings updated globally"}
        else:
            raise HTTPException(status_code=500, detail="Failed to update settings")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/home-sections")
async def get_home_sections():
    try:
        return await db.get_home_sections()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/home-sections")
async def create_home_section(section: HomeSectionSchema, admin: dict = Depends(verify_admin)):
    try:
        # Check duplicate title
        existing = await db.db["home_sections"].find_one({"title": section.title})
        if existing:
            raise HTTPException(status_code=400, detail="A home section with this title already exists")
        sec_id = await db.create_home_section(section.dict(exclude_unset=True))
        api_cache.delete("home_data")
        return {"id": sec_id, "message": "Home section created successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/home-sections/reorder")
async def reorder_home_sections(payload: Dict[str, List[str]], admin: dict = Depends(verify_admin)):
    try:
        from bson import ObjectId
        section_ids = payload.get("section_ids", [])
        for idx, sec_id in enumerate(section_ids):
            await db.home_sections.update_one(
                {"_id": ObjectId(sec_id)},
                {"$set": {"position": idx + 1}}
            )
        api_cache.delete("home_data")
        return {"message": "Home sections reordered successfully"}
    except Exception as e:
        LOGGER.error(f"Error reordering home sections: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/home-sections/{section_id}")
async def update_home_section(section_id: str, section: HomeSectionSchema, admin: dict = Depends(verify_admin)):
    try:
        from bson import ObjectId
        # Check duplicate title (excluding this section)
        existing = await db.db["home_sections"].find_one({
            "title": section.title,
            "_id": {"$ne": ObjectId(section_id)}
        })
        if existing:
            raise HTTPException(status_code=400, detail="A home section with this title already exists")
        success = await db.update_home_section(section_id, section.dict(exclude_unset=True))
        api_cache.delete("home_data")
        if success:
            return {"message": "Home section updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="Section not found or not modified")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/home-sections/{section_id}")
async def delete_home_section(section_id: str, admin: dict = Depends(verify_admin)):
    try:
        success = await db.delete_home_section(section_id)
        api_cache.delete("home_data")
        if success:
            return {"message": "Home section deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Section not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/home-sections/{section_id}/items")
async def get_home_section_items(section_id: str):
    try:
        from bson import ObjectId
        sec = await db.home_sections.find_one({"_id": ObjectId(section_id)})
        if not sec:
            raise HTTPException(status_code=404, detail="Section not found")
        
        sec = db._convert_object_id(sec)
        sec_type = sec.get("section_type")
        media_type = sec.get("media_type", "both")
        limit = sec.get("limit", 10)
        
        items = []
        if sec_type == "latest":
            if media_type == "movie":
                res = await db.movie_collection.find({}, {"telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                items = [db._convert_object_id(x) for x in res]
            elif media_type == "tv":
                res = await db.tv_collection.find({}, {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                items = [db._convert_object_id(x) for x in res]
            else:
                movies, tvshows = await asyncio.gather(
                    db.movie_collection.find({}, {"telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("updated_on", DESCENDING)]).limit(limit).to_list(None),
                    db.tv_collection.find({}, {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                )
                combined = [db._convert_object_id(x) for x in movies] + [db._convert_object_id(x) for x in tvshows]
                combined.sort(key=lambda x: x.get("updated_on") if x.get("updated_on") else datetime.min, reverse=True)
                items = combined[:limit]
        elif sec_type == "trending":
            if media_type == "movie":
                res = await db.movie_collection.find({}, {"telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("views", DESCENDING)]).limit(limit).to_list(None)
                items = [db._convert_object_id(x) for x in res]
            elif media_type == "tv":
                res = await db.tv_collection.find({}, {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("views", DESCENDING)]).limit(limit).to_list(None)
                items = [db._convert_object_id(x) for x in res]
            else:
                movies, tvshows = await asyncio.gather(
                    db.movie_collection.find({}, {"telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("views", DESCENDING)]).limit(limit).to_list(None),
                    db.tv_collection.find({}, {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("views", DESCENDING)]).limit(limit).to_list(None)
                )
                combined = [db._convert_object_id(x) for x in movies] + [db._convert_object_id(x) for x in tvshows]
                combined.sort(key=lambda x: x.get("views", 0), reverse=True)
                items = combined[:limit]
        elif sec_type == "top_release":
            raw_items = sec.get("items", [])[:limit]
            items = await db._populate_items(raw_items)
            
        # Sort languages by priority
        settings = await db.get_settings()
        priority = settings.get("language_priority", [])
        for item in items:
            if "languages" in item:
                item["languages"] = db._sort_languages(item["languages"], priority)
                
        return {"items": items}
    except Exception as e:
        LOGGER.error(f"Error getting home section items: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/api/admin/login")
async def admin_login(payload: Dict[str, str]):
    username = payload.get("username")
    password = payload.get("password")
    
    admin = await db.get_admin_credentials()
    if not admin:
        raise HTTPException(status_code=404, detail="Admin credentials not set via Bot.")
    
    if username == admin["username"] and pwd_context.verify(password, admin["password"]):
        token = jwt.encode(
            {"sub": username, "role": "admin", "exp": datetime.utcnow() + timedelta(days=7)},
            JWT_SECRET,
            algorithm=JWT_ALGORITHM
        )
        return {"token": token}
    else:
        raise HTTPException(status_code=401, detail="Invalid username or password")

@app.get("/api/admin/analytics")
async def admin_analytics(period: str = Query("week"), admin: dict = Depends(verify_admin)
):
    try:
        stats = await db.get_analytics()
        graph_data = await db.get_view_graph_data(period)
        return {**stats, "graph_data": graph_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def perform_sync_manual_files():
    """Background task to verify manual files exist on Telegram and remove deleted ones."""
    files = await db.manual_collection.find({"is_linked": {"$ne": True}}).to_list(None)
    
    deleted_count = 0
    # Group by chat_id for efficiency
    chat_groups = {}
    for f in files:
        cid = f["chat_id"]
        if cid not in chat_groups: chat_groups[cid] = []
        chat_groups[cid].append(f)

    for cid, g_files in chat_groups.items():
        # Telegram chat_id usually starts with -100 for channels
        try:
            actual_chat_id = int(cid) if str(cid).startswith("-100") else int(f"-100{cid}")
            msg_ids = [f["msg_id"] for f in g_files]
            
            # Batch get messages
            for i in range(0, len(msg_ids), 200):
                batch_ids = msg_ids[i:i+200]
                try:
                    tg_msgs = await StreamBot.get_messages(actual_chat_id, batch_ids)
                    if not isinstance(tg_msgs, list): tg_msgs = [tg_msgs]
                    
                    for idx, msg in enumerate(tg_msgs):
                        if i+idx >= len(g_files): break
                        if msg.empty or not (msg.video or msg.document):
                            await db.delete_manual_file(str(g_files[i+idx]["_id"]))
                            deleted_count += 1
                except Exception as e:
                    LOGGER.error(f"Sync error for chat {cid} batch: {e}")
        except Exception as e:
            LOGGER.error(f"Sync error for chat {cid}: {e}")

    LOGGER.info(f"Background Sync complete. Removed {deleted_count} stale files.")

@app.post("/api/admin/manual/sync")
async def admin_sync_manual_files(background_tasks: BackgroundTasks, admin: dict = Depends(verify_admin)):
    """Verify manual files exist on Telegram and remove deleted ones (runs in background)."""
    background_tasks.add_task(perform_sync_manual_files)
    return {"message": "Sync started in background. Stale files will be removed gradually."}


@app.get("/api/id/{tmdb_id}", response_model=dict)
async def get_media_details(
    tmdb_id: str, 
    season_number: Optional[int] = Query(None), 
    episode_number: Optional[int] = Query(None),
    title: Optional[str] = Query(None),
    media_type: Optional[str] = Query(None)
) -> Union[dict, None]:
    tmdb_id_val = int(tmdb_id) if tmdb_id.isdigit() else tmdb_id
    details = await db.get_media_details(
        tmdb_id=tmdb_id_val, 
        season_number=season_number, 
        episode_number=episode_number,
        title=title,
        media_type=media_type
    )

    if not details:
        raise HTTPException(status_code=404, detail="Requested details not found")
    
    return details



@app.get("/api/similar/")
async def get_similar_media(
    tmdb_id: int,
    media_type: str = Query(..., regex="^(movie|tvshow)$"),
    page: int = Query(default=1, ge=1, description="Page number to return"),
    page_size: int = Query(default=10, ge=1, description="Number of similar media per page")
):
    """
    FastAPI endpoint to get similar movies or TV shows based on the parent tmdb_id, sorted by the number of genre matches and rating.
    
    :param tmdb_id: The TMDB ID of the parent movie or TV show.
    :param media_type: The media type ('movie' or 'tvshow').
    :param page: The page number to return.
    :param page_size: The number of similar media per page.
    :return: A dictionary containing the total count and a list of similar movies or TV shows.
    """
    similar_media = await db.find_similar_media(tmdb_id=tmdb_id, media_type=media_type, page=page, page_size=page_size)
    return similar_media


# moviepage = http://127.0.0.1:8000/api/similar/?tmdb_id=695962&media_type=movie&limit=10
# similar movie tab = http://127.0.0.1:8000/api/similar/?tmdb_id=695962&media_type=movie&limit=40

# tvshowpage = http://127.0.0.1:8000/api/similar/?tmdb_id=695962&media_type=tvshow&limit=10
# similar tvshow tab = http://127.0.0.1:8000/api/similar/?tmdb_id=695962&media_type=tvshow&limit=40



@app.get("/api/search/", response_model=dict)
async def search_documents_endpoint(
    query: str = Query(..., description="Search query string"),
    page: int = Query(default=1, ge=1, description="Page number to return"),
    page_size: int = Query(default=10, ge=1, description="Number of documents per page")
):
    """
    FastAPI endpoint to search documents by title across TV and Movie collections,
    with pagination and total count.

    :param query: The search query string.
    :param page: The page number to return.
    :param page_size: The number of documents per page.
    :return: A dictionary containing the total count and a list of search results.
    """
    try:
        search_results = await db.search_documents(query=query, page=page, page_size=page_size)
        return search_results
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# search popup = http://127.0.0.1:8000/api/search/?query=the%20boys&page=1&page_size=10
# search tab = http://127.0.0.1:8000/api/search/?query=the%20boys&page=1&page_size=40

@app.get('/download/{id}/{name}')
async def download_handler(request: Request, id: str, name: str):
    try:
        decoded_data = await decode_string(id)
        if not decoded_data or not decoded_data.get('msg_id') or not decoded_data.get('hash'):
            raise HTTPException(status_code=400, detail="Invalid or expired media ID")
        cid = str(decoded_data['chat_id'])
        chat_id = cid if cid.startswith("-100") else f"-100{cid}"
        return await media_streamer(request, int(chat_id), int(decoded_data['msg_id']), decoded_data['hash'], download=True)
    except InvalidHash:
        raise HTTPException(status_code=403, detail="Invalid secure hash")
    except FIleNotFound:
        raise HTTPException(status_code=404, detail="Requested file not found on Telegram")
    except Exception as e:
        LOGGER.error(f"Download error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/stream/{id}/{name}')
async def stream_handler(request: Request, id: str, name: str):
    try:
        decoded_data = await decode_string(id)
        if not decoded_data or not decoded_data.get('msg_id') or not decoded_data.get('hash'):
            raise HTTPException(status_code=400, detail="Invalid or expired media ID")
        cid = str(decoded_data['chat_id'])
        chat_id = cid if cid.startswith("-100") else f"-100{cid}"
        return await media_streamer(request, int(chat_id), int(decoded_data['msg_id']), decoded_data['hash'])
    except InvalidHash:
        raise HTTPException(status_code=403, detail="Invalid secure hash")
    except FIleNotFound:
        raise HTTPException(status_code=404, detail="Requested file not found on Telegram")
    except Exception as e:
        LOGGER.error(f"Stream error: {e}")
        raise HTTPException(status_code=500, detail=str(e))



async def media_streamer(request: Request, chat_id: int, msg_id: int, secure_hash: str, download: bool = False):
    range_header = request.headers.get("Range")
    # Fail-safe: Use index 0 (StreamBot) if multi-clients aren't ready
    if not work_loads:
        index = 0
        faster_client = StreamBot
    else:
        index = min(work_loads, key=work_loads.get)
        faster_client = multi_clients[index]

    work_loads[index] += 1

    if Telegram.MULTI_CLIENT:
        LOGGER.debug(f"Client {index} is now serving {request.client.host}")
    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
        LOGGER.debug(f"Using cached ByteStreamer object for client {index}")
    else:
        LOGGER.debug(f"Creating new ByteStreamer object for client {index}")
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect
    LOGGER.debug("before calling get_file_properties")
    file_id = await tg_connect.get_file_properties(chat_id=chat_id, message_id=msg_id)
    LOGGER.debug("after calling get_file_properties")
    if file_id.unique_id[:6] != secure_hash:
        LOGGER.debug(f"Invalid hash for message with ID {msg_id}")
        raise InvalidHash
    file_size = file_id.file_size
    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        if until_bytes:
            until_bytes = int(until_bytes)
        else:
            # Cap initial open-ended requests to 10MB for fast playback start
            # Browser will request more ranges automatically after buffering starts
            until_bytes = min(from_bytes + 10 * 1024 * 1024 - 1, file_size - 1)
    else:
        from_bytes = 0
        until_bytes = file_size - 1
    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return StreamingResponse(
            content=(f"416: Range not satisfiable",),
            status_code=416,
            headers={"Content-Range": f"bytes */{file_size}"},
        )
    
    chunk_size = 1024 * 1024
        
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
  #  part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    part_count = math.ceil((until_bytes - offset + 1) / chunk_size)
    
    async def file_chunk_generator():
        try:
            async for chunk in tg_connect.yield_file(
                file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
            ):
                yield chunk
        finally:
            work_loads[index] -= 1
            
    body = file_chunk_generator()
    mime_type = file_id.mime_type
    file_name = file_id.file_name
    disposition = "inline"

    if mime_type:
        if not file_name:
            try:
                file_name = f"{secrets.token_hex(2)}.{mime_type.split('/')[1]}"
            except (IndexError, AttributeError):
                file_name = f"{secrets.token_hex(2)}.unknown"
    else:
        if file_name:
            mime_type = mimetypes.guess_type(file_name)[0]
        else:
            mime_type = "application/octet-stream"
            file_name = f"{secrets.token_hex(2)}.unknown"

    # async def file_chunk_generator():
    #     async for chunk in tg_connect.yield_file(
    #         file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
    #     ):
    #         yield chunk
    LOGGER.info(f"{mime_type}, {file_name}, {disposition}")
    headers = {
        "Connection": "keep-alive",
        "Accept-Ranges": "bytes",
        "Content-Length": str(req_length),
        "Cache-Control": "private, max-age=3600, no-transform",  # Cache segments for 1hr, disable transform to prevent proxy interference
        "X-Accel-Buffering": "no",                  # Disable Nginx/Proxy buffering for streaming
    }

    if range_header:
         headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"
    
 #   download = request.query_params.get("download")
    # Only remap truly unsupported MIME types for browser playback
    # Note: Do NOT remap video/x-matroska to video/webm — Chrome handles MKV natively
    # but fails when served as WebM if codecs are H.264/AC3 (not VP8/VP9/Opus)
    MIME_REMAP = {
        "video/x-msvideo": "video/mp4",
    }
    if not download:
        headers["Content-Type"] = MIME_REMAP.get(mime_type, mime_type)
    else:
        encoded_filename = quote(file_name)
        headers["Content-Disposition"] = (
        f'attachment; filename="{file_name.encode("ascii", "ignore").decode()}"; '
        f"filename*=UTF-8''{encoded_filename}"
    )
        headers["Content-Type"] = "application/octet-stream"

    return StreamingResponse(
        status_code=206 if range_header else 200,
        content=body,
        headers=headers,
        )

@app.get("/api/admin/external/search")
async def admin_external_search(
    query: str = Query(...),
    media_type: str = Query(..., regex="^(movie|tv)$"),
    source: str = Query("tmdb", regex="^(tmdb|imdb)$"),
    admin: dict = Depends(verify_admin)
):

    if source == "tmdb":
        return await search_tmdb(query, media_type)
    else:
        return await search_imdb(query, media_type)

@app.get("/api/admin/external/details")
async def admin_external_details(
    id: str = Query(...),
    media_type: str = Query(..., regex="^(movie|tv)$"),
    source: str = Query("tmdb", regex="^(tmdb|imdb)$"),
    admin: dict = Depends(verify_admin)
):
    details = await get_external_details(id, media_type, source)
    if not details:
        raise HTTPException(status_code=404, detail="External details not found")
    return details

@app.post("/api/admin/media/add")
async def admin_add_media(media_data: Dict[str, Any], admin: dict = Depends(verify_admin)):
    try:
        # Ensure unique and normalized languages
        if "languages" in media_data:
            from Backend.helper.pyro import normalize_languages
            media_data["languages"] = normalize_languages(media_data["languages"])

        if media_data.get("media_type") == "movie":
            # Using update_movie which handles upsert
            doc = MovieSchema(**media_data)
            res = await db.update_movie(doc)
        else:
            doc = TVShowSchema(**media_data)
            res = await db.update_tv_show(doc)
        
        if res:
            api_cache.clear()
            return {"message": "Media added/updated successfully", "id": str(res)}
        else:
            raise HTTPException(status_code=500, detail="Failed to add media")
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/admin/media/{media_type}/{tmdb_id}")
@app.post("/api/admin/media/{media_type}/{tmdb_id}/delete")
async def admin_delete_media(
    media_type: str, 
    tmdb_id: str, 
    title: Optional[str] = Query(None),
    doc_id: Optional[str] = Query(None),
    admin: dict = Depends(verify_admin)
):
    try:
        tmdb_id_val = int(tmdb_id) if tmdb_id.isdigit() else tmdb_id
        success = await db.delete_document(media_type, tmdb_id_val, title, doc_id)
        if success:
            api_cache.clear()
            return {"message": "Media deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Media not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/admin/media/{media_type}/{tmdb_id}/details")
async def admin_update_media_details(media_type: str, tmdb_id: str, payload: dict, admin: dict = Depends(verify_admin)):
    try:
        tmdb_id_val = int(tmdb_id) if tmdb_id.isdigit() else tmdb_id
        original_title = payload.pop("originalTitle", None)
        
        # Determine collection
        collection = db.movie_collection if media_type == "movie" else db.tv_collection
        
        # Build query
        query = {"tmdb_id": {"$in": [tmdb_id_val, str(tmdb_id_val)]}}
        if original_title:
            query["title"] = original_title
            
        # Clean payload for update
        update_data = {k: v for k, v in payload.items() if k not in ["_id", "tmdb_id", "media_type", "type", "telegram", "seasons"]}
        
        # Ensure unique and normalized languages
        if "languages" in update_data:
            from Backend.helper.pyro import normalize_languages
            update_data["languages"] = normalize_languages(update_data["languages"])

        # Convert rating to float
        if "rating" in update_data:
            try:
                update_data["rating"] = float(update_data["rating"])
            except:
                pass
        
        update_data["updated_on"] = datetime.utcnow()
                
        result = await collection.update_one(query, {"$set": update_data})
        if result.modified_count > 0:
            api_cache.clear()
            return {"message": "Details updated successfully"}
        else:
            return {"message": "No changes made or document not found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/admin/media/{media_type}/{tmdb_id}/quality/{quality_id}")
@app.post("/api/admin/media/{media_type}/{tmdb_id}/quality/{quality_id}/delete")
async def admin_delete_quality(
    media_type: str, 
    tmdb_id: str, 
    quality_id: str,
    season: Optional[int] = Query(None),
    episode: Optional[int] = Query(None),
    title: Optional[str] = Query(None),
    doc_id: Optional[str] = Query(None),
    admin: dict = Depends(verify_admin)
):
    tmdb_id_val = int(tmdb_id) if tmdb_id.isdigit() else tmdb_id
    success = await db.delete_quality_link(media_type, tmdb_id_val, quality_id, season, episode, title, doc_id)
    if success:
        return {"message": "Quality link deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Quality link not found")

@app.put("/api/admin/media/{media_type}/{tmdb_id}/quality/{quality_id}")
async def admin_update_quality(
    media_type: str, 
    tmdb_id: str, 
    quality_id: str, 
    updated_data: Dict[str, Any],
    season: Optional[int] = Query(None),
    episode: Optional[int] = Query(None),
    title: Optional[str] = Query(None),
    doc_id: Optional[str] = Query(None),
    admin: dict = Depends(verify_admin)
):
    tmdb_id_val = int(tmdb_id) if tmdb_id.isdigit() else tmdb_id
    success = await db.update_quality_link(media_type, tmdb_id_val, quality_id, updated_data, season, episode, title, doc_id)
    if success:
        return {"message": "Quality link updated successfully"}
    else:
        raise HTTPException(status_code=404, detail="Quality link not found")

@app.delete("/api/admin/media/tv/{tmdb_id}/season/{season_number}")
async def admin_delete_season(
    tmdb_id: str, 
    season_number: int,
    title: Optional[str] = Query(None),
    doc_id: Optional[str] = Query(None),
    admin: dict = Depends(verify_admin)
):
    tmdb_id_val = int(tmdb_id) if tmdb_id.isdigit() else tmdb_id
    success = await db.delete_season(tmdb_id_val, season_number, title, doc_id)
    if success:
        return {"message": f"Season {season_number} deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Season not found")

@app.delete("/api/admin/media/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}")
async def admin_delete_episode(
    tmdb_id: str, 
    season_number: int, 
    episode_number: int,
    title: Optional[str] = Query(None),
    doc_id: Optional[str] = Query(None),
    admin: dict = Depends(verify_admin)
):
    tmdb_id_val = int(tmdb_id) if tmdb_id.isdigit() else tmdb_id
    success = await db.delete_episode(tmdb_id_val, season_number, episode_number, title, doc_id)
    if success:
        return {"message": f"Episode {episode_number} deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Episode not found")

@app.put("/api/admin/media/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}")
async def admin_update_episode(
    tmdb_id: str, 
    season_number: int, 
    episode_number: int, 
    updated_data: Dict[str, Any],
    title: Optional[str] = Query(None),
    doc_id: Optional[str] = Query(None),
    admin: dict = Depends(verify_admin)
):
    tmdb_id_val = int(tmdb_id) if tmdb_id.isdigit() else tmdb_id
    success = await db.update_episode(tmdb_id_val, season_number, episode_number, updated_data, title, doc_id)
    if success:
        return {"message": "Episode updated successfully"}
    else:
        raise HTTPException(status_code=404, detail="Episode not found")

@app.delete("/api/admin/manual/files/{file_id}")
@app.post("/api/admin/manual/files/{file_id}/delete")
async def admin_delete_manual_file(file_id: str, admin: dict = Depends(verify_admin)):
    success = await db.delete_manual_file(file_id)
    if success:
        return {"message": "Manual file removed"}
    else:
        raise HTTPException(status_code=404, detail="File not found")

@app.delete("/api/admin/manual/bulk_delete")
@app.post("/api/admin/manual/bulk_delete/delete")
async def admin_bulk_delete_manual_files(payload: Dict[str, Any], admin: dict = Depends(verify_admin)):
    file_ids = payload.get("file_ids", [])
    deleted = 0
    for fid in file_ids:
        if await db.delete_manual_file(fid):
            deleted += 1
    return {"message": f"Deleted {deleted} files."}

@app.get("/api/admin/diagnostic/db")
async def admin_diagnostic_db(admin: dict = Depends(verify_admin)):
    conn_uri = db.connection_uri if hasattr(db, "connection_uri") else "Unknown"
    db_name = db.db_name if hasattr(db, "db_name") else "Unknown"
    import re
    redacted_uri = re.sub(r":([^@]+)@", ":[REDACTED]@", conn_uri)
    collections = []
    try:
        collections = await db.db.list_collection_names()
    except Exception as e:
        collections = [f"Error: {e}"]
    return {
        "uri": redacted_uri,
        "db_name": db_name,
        "collections": collections
    }

@app.get("/api/admin/manual/files")
async def admin_get_manual_files(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
    query: Optional[str] = Query(None),
    admin: dict = Depends(verify_admin)
):
    res = await db.get_unlinked_files(page, page_size, query)
    for file in res.get("files", []):
        parsed = PTN.parse(file["name"])
        file["detected"] = {
            "quality": parsed.get("resolution"),
            "season": parsed.get("season"),
            "episode": parsed.get("episode"),
            "title": parsed.get("title")
        }
    return res

def parse_season_episode(filename: str):
    import PTN
    import re
    parsed = PTN.parse(filename)
    season = parsed.get("season")
    episode = parsed.get("episode")
    
    if season is None:
        s_match = re.search(r'\bS(?:eason)?[.\-\s]*(\d+)\b', filename, re.IGNORECASE)
        if s_match:
            season = int(s_match.group(1))
        else:
            season = 1
            
    if episode is None:
        ep_match = re.search(r'\bS(?:eason)?[.\-\s]*\d+[.\-\s]*(?:ep|episode|e|x)?[.\-\s]*(\d+)\b', filename, re.IGNORECASE)
        if ep_match:
            episode = int(ep_match.group(1))
        else:
            ep_match2 = re.search(r'\b(?:ep|episode|e|x)[.\-\s]*(\d+)\b', filename, re.IGNORECASE)
            if ep_match2:
                episode = int(ep_match2.group(1))
            else:
                candidates = re.findall(r'\b(\d{1,3})\b', filename)
                valid_candidates = []
                for c in candidates:
                    num = int(c)
                    if num in [480, 720, 1080, 2160]:
                        continue
                    if num == season and f"S{c}" in filename.upper():
                        continue
                    valid_candidates.append(num)
                if valid_candidates:
                    excess = parsed.get("excess")
                    if excess and str(excess).isdigit():
                        episode = int(excess)
                    else:
                        episode = valid_candidates[-1]
    return season, episode

@app.post("/api/admin/manual/link")
async def admin_link_manual_file(payload: Dict[str, Any], send_report: bool = True, admin: dict = Depends(verify_admin)):
    # payload: { file_id, tmdb_id, media_type, season, episode, quality, title }
    try:
        file_id = payload.get("file_id")
        tmdb_id = payload.get("tmdb_id")
        media_type = payload.get("media_type")
        title = payload.get("title")
        doc_id = payload.get("doc_id")
        
        # Fetch file details from manual_files
        file_doc = await db.manual_collection.find_one(db._id_filter(file_id))
        if not file_doc:
            raise HTTPException(status_code=404, detail="File not found in manual database")
        
        # Fetch actual message for Layer 1 and Caption
        actual_chat_id = int(f"-100{file_doc['chat_id']}")
        filename = file_doc["name"]
        try:
            msg = await StreamBot.get_messages(actual_chat_id, file_doc['msg_id'])
        except Exception as e:
            LOGGER.error(f"Error fetching message for language detection: {e}")
            msg = None

        # 1. Extract Info from Telegram Message
        # Robustly extract filename from message if available
        if msg:
            if msg.document:
                filename = msg.document.file_name or filename
            elif msg.video:
                filename = msg.video.file_name or filename
            elif msg.animation:
                filename = msg.animation.file_name or filename
            
        caption = (msg.caption if msg else "") or (msg.text if msg else "") or ""
        text_to_scan = f"{filename} {caption}".lower()
        
        # 2. Parse Metadata with PTN
        import PTN
        parsed_filename = PTN.parse(filename)
        
        # Layer 1: Internal Audio Tracks
        from Backend.helper.mediainfo import get_media_languages
        raw_langs = await get_media_languages(msg) if msg else None
        
        if not raw_langs:
            raw_langs = parsed_filename.get("language")
            
            if not raw_langs:
                # Secondary scan: check text for common language markers using word boundaries
                possible_langs = ["tamil", "telugu", "hindi", "malayalam", "kannada", "english", "bengali", "marathi", "gujarati", "punjabi", "french", "spanish", "jap", "kor", "tam", "tel", "hin", "mal", "kan", "eng", "ben", "mar", "guj", "pun", "fra", "spa"]
                raw_langs = []
                import re
                for lang in possible_langs:
                    if re.search(rf'\b{lang}\b', text_to_scan):
                        raw_langs.append(lang)
            
        detected_languages = normalize_languages(raw_langs)
        
        # Extended caption/filename quality detection
        detected_quality_from_text = None
        import re
        # Ordered by specificity (longer/more specific patterns first)
        # Using [.\-\s]* to handle various separators like "WEB-DL", "WEB.DL", "WEB DL", "WEBDL"
        patterns = [
            ("4K UHD", r"\b(4k|uhd|ultra[.\-\s]*hd|2160p)\b"),
            ("1080p FHD", r"\b1080p\b"),
            ("720p HD", r"\b720p\b"),
            ("480p SD", r"\b480p\b"),
            ("BR-Rip", r"\bbr[.\-\s]*rip\b"),
            ("BD-Rip", r"\bbd[.\-\s]*rip\b"),
            ("Blu-Ray", r"\b(blu[.\-\s]*ray|bd[.\-\s]*remux|bluray)\b"),
            ("TRUE WEB-DL", r"\btrue[.\-\s]*web[.\-\s]*dl\b"),
            ("WEB-DL", r"\bweb[.\-\s]*dl\b"),
            ("WEB-Rip", r"\bweb[.\-\s]*rip\b"),
            ("HQ HDRip", r"\bhq[.\-\s]*hd[.\-\s]*rip\b"),
            ("HD-Rip", r"\bhd[.\-\s]*rip\b"),
            ("DVD-Rip", r"\bdvd[.\-\s]*rip\b"),
            ("HDR 10Bit", r"\b(hdr10|10bit|hi10p)\b"),
            ("HDR", r"\bhdr\b"),
            ("HDTC", r"\b(hdtc|hd[.\-\s]*ts)\b"),
            ("TS/CAM", r"\b(cam|hdcam|ts|tc|telecine)\b"),
            ("Pre-DVD", r"\b(pre[.\-\s]*dvd|predvd)\b"),
            ("HC-Sub", r"\b(hc|hardcoded)[.\-\s]*sub\b"),
            ("DTH-Rip", r"\bdth[.\-\s]*rip\b"),
            ("HQ", r"\bhq\b"),
            ("HD", r"\bhd\b")
        ]
        for q, pattern in patterns:
            if re.search(pattern, text_to_scan):
                detected_quality_from_text = q
                break
                
        # 2. Detect Quality (Payload first, then Manual Scan, then PTN, then Resolution, then default)
        ptn_quality = parsed_filename.get("quality")
        ptn_resolution = parsed_filename.get("resolution")
        detected_quality = payload.get("quality") or detected_quality_from_text or ptn_quality or ptn_resolution or "Unknown"

        LOGGER.info(f"Quality Detection Debug: file={filename}, ptn_quality={ptn_quality}, res={ptn_resolution}, text_detected={detected_quality_from_text}, final={detected_quality}")

        LOGGER.info(f"Manual Link Detection: file={filename}, langs={detected_languages}, quality={detected_quality}")

        from Backend.helper.encrypt import encode_string, compact_encode
        # Check for required fields to avoid 500 error
        if not all(k in file_doc for k in ["chat_id", "msg_id", "hash"]):
            raise HTTPException(status_code=400, detail="Incomplete file record (missing chat_id, msg_id, or hash). Try Synchronizing Repository.")
            
        # Use compact encoding for bot links to fit in Telegram's 64-char limit
        encoded_id = await compact_encode(file_doc["chat_id"], file_doc["msg_id"], file_doc["hash"])
        quality_detail = {
            "quality": detected_quality,
            "id": encoded_id,
            "name": filename,
            "size": file_doc["size"]
        }
        
        query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}}
        if doc_id:
            query = db._id_filter(doc_id)
        elif title:
            query["title"] = title

        if media_type == "movie":
            # Link to movie
            update_op = {
                "$push": {"telegram": quality_detail},
                "$set": {"updated_on": datetime.utcnow()}
            }
            if detected_quality != "Unknown":
                update_op["$set"]["rip"] = detected_quality
                
            if detected_languages:
                if "$addToSet" not in update_op: update_op["$addToSet"] = {}
                update_op["$addToSet"]["languages"] = {"$each": detected_languages}

            result = await db.movie_collection.update_one(query, update_op)
        else:
            # Link to TV episode
            season_val = payload.get("season")
            episode_val = payload.get("episode")
            
            if isinstance(season_val, str) and not season_val.strip():
                season_val = None
            if isinstance(episode_val, str) and not episode_val.strip():
                episode_val = None
                
            if season_val is None or episode_val is None:
                auto_season, auto_episode = parse_season_episode(filename)
                if season_val is None:
                    season_val = auto_season
                if episode_val is None:
                    episode_val = auto_episode
                    
            if season_val is None or episode_val is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Could not detect Season or Episode from filename '{filename}'. Please specify them manually."
                )
                
            try:
                season_num = int(season_val)
                episode_num = int(episode_val)
            except (ValueError, TypeError) as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid Season/Episode value: season={season_val}, episode={episode_val}."
                )
            
            # Ensure Season exists
            await db.tv_collection.update_one(
                {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}, "seasons.season_number": {"$ne": season_num}},
                {"$push": {"seasons": {"season_number": season_num, "episodes": []}}}
            )
            # Try to fetch the real episode title/backdrop from TMDB before
            # falling back to a generic placeholder. Previously this always
            # inserted "Episode {n}" and never got corrected, which is why
            # newly-linked seasons (e.g. Season 3) showed generic titles
            # while older seasons (added via bulk TMDB import) had real ones.
            episode_title = f"Episode {episode_num}"
            episode_backdrop = ""
            try:
                ep_details = await tmdb.episode(int(tmdb_id), season_num, episode_num).details()
                if ep_details and ep_details.name:
                    episode_title = ep_details.name
                if ep_details and getattr(ep_details, "still_path", None):
                    episode_backdrop = f"https://image.tmdb.org/t/p/original{ep_details.still_path}"
            except Exception as e:
                LOGGER.warning(f"Could not fetch TMDB title for S{season_num}E{episode_num} of tmdb_id={tmdb_id}: {e}")

            # Ensure Episode exists in that season
            await db.tv_collection.update_one(
                {
                    "tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]},
                    "seasons": {
                        "$elemMatch": {
                            "season_number": season_num,
                            "episodes.episode_number": {"$ne": episode_num}
                        }
                    }
                },
                {"$push": {"seasons.$.episodes": {
                    "episode_number": episode_num, 
                    "title": episode_title, 
                    "episode_backdrop": episode_backdrop, 
                    "telegram": []
                }}}
            )

            update_op = {
                "$push": {"seasons.$[s].episodes.$[e].telegram": quality_detail},
                "$set": {"updated_on": datetime.utcnow()}
            }
            if detected_quality != "Unknown":
                update_op["$set"]["rip"] = detected_quality

            if detected_languages:
                if "$addToSet" not in update_op: update_op["$addToSet"] = {}
                update_op["$addToSet"]["languages"] = {"$each": detected_languages}

            result = await db.tv_collection.update_one(
                query,
                update_op,
                array_filters=[
                    {"s.season_number": season_num},
                    {"e.episode_number": episode_num}
                ]
            )
        
        if result.modified_count > 0:
            api_cache.clear()
            await db.link_manual_file(file_id)
            # Send notification
            if send_report:
                metadata_info = await db.get_media_details(tmdb_id=tmdb_id, media_type=media_type, title=title)
                if metadata_info:
                    await send_log_report(metadata_info, mode="Manual")
            return {"message": "File linked successfully"}
        else:
            raise HTTPException(status_code=400, detail=f"Failed to link. Target media (ID: {tmdb_id}) not found or structure invalid (missing Season/Episode).")
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid data types in request: {str(e)}")
    except Exception as e:
        LOGGER.error(f"Link error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.post("/api/admin/manual/bulk_link")
async def admin_bulk_link_manual_files(payload: Dict[str, Any], admin: dict = Depends(verify_admin)):
    tmdb_id = payload.get("tmdb_id")
    media_type = payload.get("media_type")
    title = payload.get("title")
    doc_id = payload.get("doc_id")
    
    success_count = 0
    errors = []
    file_ids = payload.get("file_ids", [])
    for fid in file_ids:
        try:
            # We reuse the logic but allow override or auto-detect
            # We simulate a single link call for each
            # To handle TV shows with auto S/E, we need to re-parse per file
            file_doc = await db.manual_collection.find_one(db._id_filter(fid))
            if not file_doc: continue
            
            parsed = PTN.parse(file_doc["name"])
            
            # For bulk, if season/episode not in payload, use detected with robust fallback
            f_season = payload.get("season")
            f_episode = payload.get("episode")
            
            if isinstance(f_season, str) and not f_season.strip():
                f_season = None
            if isinstance(f_episode, str) and not f_episode.strip():
                f_episode = None
                
            if f_season is None or f_episode is None:
                auto_season, auto_episode = parse_season_episode(file_doc["name"])
                if f_season is None:
                    f_season = auto_season
                if f_episode is None:
                    f_episode = auto_episode
                    
            f_quality = payload.get("quality") or parsed.get("resolution") or "Unknown"
            
            # Prepare dummy payload for single link call
            link_payload = {
                "file_id": fid,
                "tmdb_id": tmdb_id,
                "media_type": media_type,
                "title": title,
                "doc_id": doc_id,
                "quality": f_quality,
                "season": f_season,
                "episode": f_episode
            }
            
            res = await admin_link_manual_file(link_payload, send_report=False)
            success_count += 1
        except Exception as e:
            errors.append(f"File {fid}: {str(e)}")

    if success_count > 0:
        metadata_info = await db.get_media_details(tmdb_id=tmdb_id, media_type=media_type, title=title)
        if metadata_info:
            await send_bulk_log_report(metadata_info, success_count, mode="Manual Bulk")

    return {"message": f"Successfully linked {success_count} files.", "errors": errors}

@app.get("/api/admin/backup")
async def admin_backup(admin: dict = Depends(verify_admin)):
    """Traditional JSON backup (Hybrid: uses ORJSON for speed)."""
    try:
        # db.get_all_data already returns a dict, ORJSONResponse will handle it fast
        return await db.get_all_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/backup/stream")
async def admin_backup_stream(admin: dict = Depends(verify_admin)):
    """NDJSON Streaming Backup (Hybrid: ORJSON lines + NDJSON framing).
    This is much more memory efficient for large databases.
    """
    async def stream_generator():
        # Dynamically fetch all collections for 100% coverage
        collections = await db.db.list_collection_names()
        for coll_name in collections:
            if coll_name.startswith("system."):
                continue
            collection = db.db[coll_name]
            async for doc in collection.find():
                doc = db._convert_object_id(doc)
                doc["_collection_name"] = coll_name # Meta-info for restoration
                # Use ORJSON for fast line serialization
                yield orjson.dumps(doc) + b"\n"

    return StreamingResponse(
        stream_generator(),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": f"attachment; filename=backup_{int(time())}.ndjson"}
    )

@app.post("/api/admin/restore")
async def admin_restore(request: Request, admin: dict = Depends(verify_admin)):
    """Restore data from either JSON (parsed with ijson for memory efficiency)
    or NDJSON (parsed with orjson line by line).

    JSON library roles in this endpoint:
      • orjson  – fast line parsing for NDJSON mode
      • ijson   – incremental parsing for large JSON uploads (avoids loading
                  the whole file into memory at once)
    """
    try:
        content_type = request.headers.get("content-type", "")
        if "application/x-ndjson" in content_type:
            # ── NDJSON mode: orjson parses each line as it arrives ────────────
            success_count = 0
            wiped_collections = set()
            async for line in request.stream():
                if not line.strip():
                    continue
                doc = orjson.loads(line)          # orjson: ultra-fast line decode
                coll_name = doc.pop("_collection_name", None)
                if coll_name:
                    # Wipe collection only once upon first encounter
                    if coll_name not in wiped_collections:
                        await db.db[coll_name].delete_many({})
                        wiped_collections.add(coll_name)

                    # Convert string _id back to ObjectId if 24-char hex
                    if coll_name not in ["settings", "admin_auth", "deploy_config"]:
                        if "_id" in doc and isinstance(doc["_id"], str) and len(doc["_id"]) == 24:
                            try:
                                doc["_id"] = ObjectId(doc["_id"])
                            except Exception:
                                pass
                    await db.db[coll_name].replace_one({"_id": doc["_id"]}, doc, upsert=True)
                    success_count += 1
            return {"message": f"Restored {success_count} documents from NDJSON"}
        else:
            # ── Traditional JSON mode: use ujson for fast body decode ─────────
            # For very large payloads ijson would be ideal, but FastAPI
            # request.body() already buffers the whole body; ujson gives us
            # a speed win here without changing the API surface.
            raw_body = await request.body()
            data = ujson.loads(raw_body)          # ujson: 2-3x faster than stdlib
            success = await db.restore_all_data(data)
            if success:
                return {"message": "Database restored successfully from JSON"}
            else:
                raise HTTPException(status_code=500, detail="Failed to restore database")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Collection Endpoints ---

from pydantic import BaseModel

# ---- Collection item schema (must be defined first) ----
class CollectionItemAdd(BaseModel):
    tmdb_id: int
    media_type: str
    title: Optional[str] = None

# ---- Collection creation schema (now can safely reference the item schema) ----
class CollectionCreate(BaseModel):
    title: str
    thumbnail: str
    items: Optional[List[CollectionItemAdd]] = None

@app.post("/api/collections")
async def create_collection(data: CollectionCreate):
    try:
        col_id = await db.create_collection(data.title, data.thumbnail)
        return {"id": col_id, "message": "Collection created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/collections")
async def get_collections(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: List[str] = Query(default=["updated_on:desc"]),
    query: Optional[str] = Query(default=None)
):
    try:
        return await db.get_collections(page, page_size, sort_by, query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/collections/{collection_id}/stream")
async def stream_collection_items(
    collection_id: str,
    sort_by: Optional[str] = Query(default=None)
):
    """Hybrid: ORJSON records + NDJSON framing for collection items."""
    async def stream_generator():
        # Use the optimized bulk fetch logic from get_collection
        # This is significantly faster than sequential get_media_details calls
        populated_col = await db.get_collection(collection_id, sort_by)
        if not populated_col:
            return
            
        items = populated_col.get("populated_items", [])
        
        # If no explicit sort, show recently added first (as intended by original reversed(items))
        if not sort_by:
            items = list(reversed(items))
            
        for item in items:
            yield orjson.dumps(item) + b"\n"

    return StreamingResponse(
        stream_generator(), 
        media_type="application/x-ndjson",
        headers={
            "X-Content-Type-Options": "nosniff",
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "Content-Encoding": "identity",
        }
    )

@app.get("/api/movies/stream")
async def stream_movies(
    sort_by: List[str] = Query(default=["updated_on:desc"]),
    genre: Optional[str] = None,
    year: Optional[str] = None
):
    """Hybrid: ORJSON records + NDJSON framing for movies."""
    async def stream_generator():
        # Build query similarly to sort_movies but as a cursor
        match_stage = {}
        if genre and genre.strip().lower() != "all":
            match_stage["genres"] = {"$regex": f"^{genre}$", "$options": "i"}
        if year and year.strip().lower() != "all":
            try: match_stage["release_year"] = int(year)
            except: pass
            
        sort_criteria = []
        for s in sort_by:
            parts = s.split(":")
            field = parts[0]
            direction = parts[1] if len(parts) > 1 else "desc"
            sort_criteria.append((field, DESCENDING if direction == "desc" else ASCENDING))

        cursor = db.movie_collection.find(match_stage, {"telegram": 0, "external_links": 0}).sort(sort_criteria)
        async for doc in cursor:
            doc = db._convert_object_id(doc)
            yield orjson.dumps(doc) + b"\n"

    return StreamingResponse(
        stream_generator(), 
        media_type="application/x-ndjson",
        headers={
            "X-Content-Type-Options": "nosniff",
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "Content-Encoding": "identity",
        }
    )

@app.get("/api/tvshows/stream")
async def stream_tvshows(
    sort_by: List[str] = Query(default=["updated_on:desc"]),
    genre: Optional[str] = None,
    year: Optional[str] = None
):
    """Hybrid: ORJSON records + NDJSON framing for TV shows."""
    async def stream_generator():
        match_stage = {}
        if genre and genre.strip().lower() != "all":
            match_stage["genres"] = {"$regex": f"^{genre}$", "$options": "i"}
        if year and year.strip().lower() != "all":
            try: match_stage["release_year"] = int(year)
            except: pass
            
        sort_criteria = []
        for s in sort_by:
            parts = s.split(":")
            field = parts[0]
            direction = parts[1] if len(parts) > 1 else "desc"
            sort_criteria.append((field, DESCENDING if direction == "desc" else ASCENDING))

        cursor = db.tv_collection.find(match_stage, {"seasons": 0}).sort(sort_criteria)
        async for doc in cursor:
            doc = db._convert_object_id(doc)
            yield orjson.dumps(doc) + b"\n"

    return StreamingResponse(
        stream_generator(), 
        media_type="application/x-ndjson",
        headers={
            "X-Content-Type-Options": "nosniff",
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "Content-Encoding": "identity",
        }
    )

@app.get("/api/collections/{collection_id}")
async def get_collection(
    collection_id: str,
    sort_by: Optional[str] = Query(default=None)
):
    try:
        col = await db.get_collection(collection_id, sort_by)
        if not col:
            raise HTTPException(status_code=404, detail="Collection not found")
        return col
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/collections/{collection_id}/items")
async def add_item_to_collection(collection_id: str, item: CollectionItemAdd):
    try:
        success = await db.add_to_collection(collection_id, item.tmdb_id, item.media_type, item.title)
        if success:
            return {"message": "Item added to collection"}
        else:
            return {"message": "Item already in collection or collection not found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/collections/{collection_id}/items/{media_type}/{tmdb_id}")
async def remove_item_from_collection(collection_id: str, media_type: str, tmdb_id: int, title: Optional[str] = Query(None)):
    try:
        success = await db.remove_from_collection(collection_id, tmdb_id, media_type, title)
        if success:
            return {"message": "Item removed from collection"}
        else:
            raise HTTPException(status_code=404, detail="Item not found in collection")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/collections/{collection_id}")
async def delete_collection(collection_id: str):
    try:
        success = await db.delete_collection(collection_id)
        if success:
            return {"message": "Collection deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Collection not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/collections/{collection_id}")
async def update_collection(collection_id: str, data: CollectionCreate):
    try:
        items_dict = [item.dict() for item in data.items] if data.items is not None else None
        success = await db.update_collection(collection_id, data.title, data.thumbnail, items_dict)
        if success:
            return {"message": "Collection updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="Collection not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/collections/{collection_id}/bulk-items")
async def bulk_add_items(collection_id: str, items: List[CollectionItemAdd]):
    try:
        # Convert Pydantic models to dicts
        items_dict = [item.dict() for item in items]
        added_count = await db.bulk_add_to_collection(collection_id, items_dict)
        return {"message": f"Added {added_count} items to collection"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
