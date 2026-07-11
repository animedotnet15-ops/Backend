# ─────────────────────────────────────────────────────────────────────────────
# Author  : ThiruXD
# GitHub  : https://github.com/ThiruXD
# Portfolio: https://thiruxd.is-a.dev
# ─────────────────────────────────────────────────────────────────────────────
from datetime import datetime, timedelta
import re
import asyncio
from typing import Dict, List, Optional, Tuple, Union
from bson import ObjectId
from fastapi import HTTPException
import motor.motor_asyncio
from pydantic import ValidationError
from pymongo import ASCENDING, DESCENDING

from Backend.logger import LOGGER
from Backend.config import Telegram
from Backend.helper.encrypt import encode_string, compact_encode
from Backend.helper.modal import Episode, MovieSchema, QualityDetail, Season, TVShowSchema, SettingsSchema, MovieListSchema, TVShowListSchema


# Mapping for expanding language codes to multiple variations for filtering
AUDIO_MAP = {
    "ta": ["ta", "tam", "tamil"],
    "hi": ["hi", "hin", "hindi"],
    "en": ["en", "eng", "english"],
    "te": ["te", "tel", "telugu"],
    "ml": ["ml", "mal", "malayalam"],
    "kn": ["kn", "kan", "kannada"],
    "bn": ["bn", "ben", "bengali"],
    "mr": ["mr", "mar", "marathi"],
    "gu": ["gu", "guj", "gujarati"],
    "pa": ["pa", "pun", "punjabi"],
    "ja": ["ja", "jap", "japanese"],
    "ko": ["ko", "kor", "korean"],
    "es": ["es", "spa", "spanish"],
    "fr": ["fr", "fra", "fre", "french"],
    "de": ["de", "ger", "german"]
}

def slugify(text: str) -> str:
    """Convert text to a URL-friendly slug."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')

def is_custom_backdrop(url: Optional[str]) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    if "image.tmdb.org" in url_lower or "media-amazon.com" in url_lower or "media-imdb.com" in url_lower:
        return False
    return True

def is_generic_title(title: Optional[str], ep_num: int) -> bool:
    if not title:
        return True
    t_lower = str(title).strip().lower()
    if t_lower == f"episode {ep_num}" or t_lower == f"episode {ep_num:02d}":
        return True
    if t_lower.startswith("episode"):
        rem = t_lower[7:].strip()
        if not rem or rem.isdigit():
            return True
    return False

def _sanitize_episodes_for_db(seasons: list, keep_custom_titles: bool = True) -> list:
    """Strips out TMDB/IMDB backdrops and handles episode titles so only custom ones are saved to DB."""
    if not seasons:
        return seasons
    sanitized_seasons = []
    for season in seasons:
        is_dict = isinstance(season, dict)
        episodes = season.get("episodes", []) if is_dict else getattr(season, "episodes", [])
        
        sanitized_episodes = []
        for ep in episodes:
            ep_is_dict = isinstance(ep, dict)
            backdrop = ep.get("episode_backdrop") if ep_is_dict else getattr(ep, "episode_backdrop", None)
            ep_num = ep.get("episode_number") if ep_is_dict else getattr(ep, "episode_number", 1)
            
            if ep_is_dict:
                clean_ep = ep.copy()
                if backdrop and not is_custom_backdrop(backdrop):
                    clean_ep["episode_backdrop"] = ""
                if not keep_custom_titles:
                    clean_ep["title"] = f"Episode {ep_num}"
                sanitized_episodes.append(clean_ep)
            else:
                if hasattr(ep, "model_copy"):
                    clean_ep = ep.model_copy()
                elif hasattr(ep, "copy"):
                    clean_ep = ep.copy()
                else:
                    import copy
                    clean_ep = copy.copy(ep)
                
                if backdrop and not is_custom_backdrop(backdrop):
                    setattr(clean_ep, "episode_backdrop", "")
                if not keep_custom_titles:
                    setattr(clean_ep, "title", f"Episode {ep_num}")
                sanitized_episodes.append(clean_ep)
        
        if is_dict:
            clean_season = season.copy()
            clean_season["episodes"] = sanitized_episodes
            sanitized_seasons.append(clean_season)
        else:
            if hasattr(season, "model_copy"):
                clean_season = season.model_copy()
            elif hasattr(season, "copy"):
                clean_season = season.copy()
            else:
                import copy
                clean_season = copy.copy(season)
            
            setattr(clean_season, "episodes", sanitized_episodes)
            sanitized_seasons.append(clean_season)
            
    return sanitized_seasons



class Database:
    def __init__(self, connection_uri: str = Telegram.DATABASE, db_name: str = "projectS"):
        self._conn = None
        self.db = None
        self.tv_collection = None
        self.movie_collection = None
        self.settings_collection = None
        self.deploy_config = None
        self.view_analytics = None
        self.watchlist = None
        self.reviews = None
        self.connection_uri = connection_uri
        self.db_name = db_name

    async def connect(self):
        """Establish a connection to the database."""
        try:
            if self._conn is not None:
                await self._conn.close()

            self._conn = motor.motor_asyncio.AsyncIOMotorClient(self.connection_uri)
            self.db = self._conn[self.db_name]

            # Ensure collections are assigned
            self.tv_collection = self.db["tv"]
            self.movie_collection = self.db["movie"]
            self.settings_collection = self.db["settings"]
            self.deploy_config = self.db["deploy_config"]
            self.manual_collection = self.db["manual_files"]
            self.view_analytics = self.db["view_analytics"]
            self.collection_collection = self.db["collections"]
            self.admin_collection = self.db["admin_auth"]
            self.home_sections = self.db["home_sections"]
            self.watchlist = self.db["watchlist"]

            LOGGER.info("Database connection established")

            await self.watchlist.create_index([("owner_id", ASCENDING), ("added_on", DESCENDING)])
            await self.watchlist.create_index(
                [("owner_id", ASCENDING), ("tmdb_id", ASCENDING), ("media_type", ASCENDING)],
                unique=True,
            )

            self.reviews = self.db["reviews"]
            await self.reviews.create_index([("tmdb_id", ASCENDING), ("media_type", ASCENDING), ("created_on", DESCENDING)])
            await self.reviews.create_index(
                [("owner_id", ASCENDING), ("tmdb_id", ASCENDING), ("media_type", ASCENDING)],
                unique=True,
            )

            # Create essential indexes to speed up sorting operations on Home page
            for coll in [self.tv_collection, self.movie_collection]:
                await coll.create_index([("updated_on", DESCENDING)])
                await coll.create_index([("views", DESCENDING)])
                await coll.create_index([("rating", DESCENDING)])
                await coll.create_index([("title", ASCENDING)])
                await coll.create_index([("slug", ASCENDING)])
                await coll.create_index([("title", "text")])
            
            await self.view_analytics.create_index([("timestamp", DESCENDING)])
            await self.view_analytics.create_index([("tmdb_id", ASCENDING), ("identifier", ASCENDING), ("timestamp", DESCENDING)])
            
            LOGGER.info("Database indices verified/created")

            # Seed default home sections if empty
            try:
                if await self.home_sections.estimated_document_count() == 0 or await self.home_sections.count_documents({"section_type": "recently_watched"}) == 0:
                    await self.home_sections.delete_many({})
                    default_sections = [
                        {
                            "title": "Recently Watched",
                            "enabled": True,
                            "section_type": "recently_watched",
                            "media_type": "both",
                            "limit": 20,
                            "layout": "slider",
                            "items": [],
                            "position": 1
                        },
                        {
                            "title": "Trending Now",
                            "enabled": True,
                            "section_type": "trending",
                            "media_type": "both",
                            "limit": 20,
                            "layout": "slider",
                            "items": [],
                            "position": 2
                        },
                        {
                            "title": "Latest Movies",
                            "enabled": True,
                            "section_type": "latest",
                            "media_type": "movie",
                            "limit": 20,
                            "layout": "grid",
                            "items": [],
                            "position": 3
                        },
                        {
                            "title": "Latest Series",
                            "enabled": True,
                            "section_type": "latest",
                            "media_type": "tv",
                            "limit": 20,
                            "layout": "grid",
                            "items": [],
                            "position": 4
                        }
                    ]
                    await self.home_sections.insert_many(default_sections)
                    LOGGER.info("Default home sections seeded successfully")
            except Exception as seed_err:
                LOGGER.error(f"Error seeding home sections: {seed_err}")

            # Debug: Print available collections
           # collections = await self.db.list_collection_names()
           # LOGGER.info(f"Available collections: {collections}")

        except Exception as e:
            LOGGER.error(f"Error connecting to the database: {e}")
            self._conn = None
            self.db = None
        

    async def disconnect(self):
        """Close the database connection."""
        if self._conn is not None:
            await self._conn.close()
            LOGGER.info("Database connection closed")
        self._conn = None
        self.db = None
        self.tv_collection = None
        self.movie_collection = None
        self.settings_collection = None
        self.collection_collection = None
        self.home_sections = None
        self.watchlist = None
        self.reviews = None

    @staticmethod
    def _convert_object_id(document: dict) -> dict:
        """Convert MongoDB ObjectId to string."""
        if "_id" in document:
            document["_id"] = str(document["_id"])
        return document

    @staticmethod
    def _id_filter(doc_id: str) -> dict:
        """Create a query dict matching either ObjectId or string format of the ID."""
        try:
            return {"_id": {"$in": [ObjectId(doc_id), doc_id]}}
        except Exception:
            return {"_id": doc_id}

    async def get_settings(self) -> dict:
        """Fetch the global settings document (returning empty dict if not found)."""
        settings = await self.settings_collection.find_one({"_id": "global_settings"})
        if settings:
            return settings
        return {}

    def _sort_languages(self, languages: List[str], priority: List[str]) -> List[str]:
        """Sort and deduplicate languages with variation support, returning pretty names."""
        if not languages:
            return []
            
        # 1. Normalize and Deduplicate variations
        normalized_map = {} # ISO -> Pretty Name
        for lang in languages:
            if not lang: continue
            l_low = lang.strip().lower()
            found_iso = None
            for iso, vars in AUDIO_MAP.items():
                if l_low == iso or l_low in [v.lower() for v in vars]:
                    found_iso = iso
                    break
            
            if found_iso:
                # Use the longest name in AUDIO_MAP as the representative "Pretty" name
                pretty = max(AUDIO_MAP[found_iso], key=len).capitalize()
                normalized_map[found_iso] = pretty
            else:
                # Keep original if not in map, but capitalize for consistency
                normalized_map[l_low] = lang.strip().capitalize()
        
        unique_pretty_names = list(normalized_map.values())
        
        if not priority:
            return sorted(unique_pretty_names)
            
        # 2. Build priority index based on normalized names
        priority_indices = {}
        for idx, p_lang in enumerate(priority):
            p_low = p_lang.strip().lower()
            found_iso = None
            for iso, vars in AUDIO_MAP.items():
                if p_low == iso or p_low in [v.lower() for v in vars]:
                    found_iso = iso
                    break
            
            if found_iso:
                pretty = max(AUDIO_MAP[found_iso], key=len).capitalize()
                priority_indices[pretty] = idx
            else:
                priority_indices[p_lang.strip().capitalize()] = idx

        def sort_key(name):
            # Sort by priority index, then alphabetically
            return priority_indices.get(name, 999), name
            
        return sorted(unique_pretty_names, key=sort_key)

    async def update_settings(self, settings_data: SettingsSchema) -> bool:
        """Upsert the global settings document."""
        try:
            settings_dict = settings_data.dict(exclude_unset=True)
            settings_dict["updated_on"] = datetime.utcnow()
            
            await self.settings_collection.update_one(
                {"_id": "global_settings"},
                {"$set": settings_dict},
                upsert=True
            )
            return True
        except Exception as e:
            LOGGER.error(f"Error updating settings: {e}")
            return False

    
    async def update_tv_show(self, tv_show_data: TVShowSchema) -> Optional[ObjectId]:
        try:
            tv_show_dict = tv_show_data.dict()
            # Ensure tmdb_id is an int
            tv_show_dict["tmdb_id"] = int(tv_show_dict["tmdb_id"])
        except (ValidationError, ValueError) as e:
            LOGGER.error(f"Validation error: {e}")
            return None

        existing_media = await self.tv_collection.find_one({
            "tmdb_id": {"$in": [int(tv_show_dict["tmdb_id"]), str(tv_show_dict["tmdb_id"])]},
            "title": tv_show_dict["title"]
        })

        # Always force updated_on to now so it appears in "Latest"
        tv_show_dict["updated_on"] = datetime.utcnow()
        tv_show_dict["slug"] = slugify(tv_show_dict["title"])

        if "seasons" in tv_show_dict:
            tv_show_dict["seasons"] = _sanitize_episodes_for_db(tv_show_dict["seasons"], keep_custom_titles=False)

        if not existing_media:
            result = await self.tv_collection.insert_one(tv_show_dict)
            return result.inserted_id

        # Update metadata
        fields_to_update = ["genres", "description", "rating", "poster", "backdrop", "total_seasons", "total_episodes", "status", "rip", "updated_on", "slug"]
        for field in fields_to_update:
            if field in tv_show_dict:
                existing_media[field] = tv_show_dict[field]

        # Merge languages
        if "languages" in tv_show_dict:
            existing_langs = set(existing_media.get("languages") or [])
            new_langs = set(tv_show_dict["languages"] or [])
            existing_media["languages"] = list(existing_langs.union(new_langs))

        # Merge seasons
        for new_season in tv_show_dict.get("seasons", []):
            existing_season = next((s for s in existing_media.get("seasons", []) if s["season_number"] == new_season["season_number"]), None)
            
            if not existing_season:
                existing_media.setdefault("seasons", []).append(new_season)
                continue
            
            # Merge episodes
            for new_episode in new_season.get("episodes", []):
                existing_episode = next((e for e in existing_season.get("episodes", []) if e["episode_number"] == new_episode["episode_number"]), None)
                
                if not existing_episode:
                    existing_season.setdefault("episodes", []).append(new_episode)
                    continue
                
                # Merge telegram streams by ID
                for new_q in new_episode.get("telegram", []):
                    existing_q = next((q for q in existing_episode.get("telegram", []) if q["id"] == new_q["id"]), None)
                    if existing_q:
                        existing_q.update(new_q)
                    else:
                        existing_episode.setdefault("telegram", []).append(new_q)
                
                # Update manual_stream_url for episode
                if "manual_stream_url" in new_episode:
                    existing_episode["manual_stream_url"] = new_episode["manual_stream_url"]

        if "seasons" in existing_media:
            existing_media["seasons"] = _sanitize_episodes_for_db(existing_media["seasons"])

        await self.tv_collection.replace_one({"_id": existing_media["_id"]}, existing_media)
        return existing_media["_id"]

    async def update_movie(self, movie_data: MovieSchema) -> Optional[ObjectId]:
        if self.movie_collection is None:
            LOGGER.error("Database collection is not initialized.")
            return None
        try:
            movie_dict = movie_data.dict()
            movie_dict["tmdb_id"] = int(movie_dict["tmdb_id"])
        except (ValidationError, ValueError) as e:
            LOGGER.error(f"Validation error: {e}")
            return None

        # Match by both ID and Title to allow intentional duplicates with different names
        # We use $in to handle cases where tmdb_id might be stored as string or int
        existing_media = await self.movie_collection.find_one({
            "tmdb_id": {"$in": [movie_dict["tmdb_id"], str(movie_dict["tmdb_id"])]},
            "title": movie_dict["title"]
        })

        # Always force updated_on to now so it appears in "Latest"
        movie_dict["updated_on"] = datetime.utcnow()
        movie_dict["slug"] = slugify(movie_dict["title"])

        if not existing_media:
            result = await self.movie_collection.insert_one(movie_dict)
            return result.inserted_id

        # Update metadata
        fields_to_update = ["genres", "description", "rating", "poster", "backdrop", "runtime", "rip", "updated_on", "slug", "manual_stream_url"]
        for field in fields_to_update:
            if field in movie_dict:
                existing_media[field] = movie_dict[field]

        # Merge languages
        if "languages" in movie_dict:
            existing_langs = set(existing_media.get("languages") or [])
            new_langs = set(movie_dict["languages"] or [])
            existing_media["languages"] = list(existing_langs.union(new_langs))

        # Merge telegram streams
        for new_q in movie_dict.get("telegram", []):
            existing_q = next((q for q in existing_media.get("telegram", []) if q["id"] == new_q["id"]), None)
            if existing_q:
                existing_q.update(new_q)
            else:
                existing_media.setdefault("telegram", []).append(new_q)

        await self.movie_collection.replace_one({"_id": existing_media["_id"]}, existing_media)
        return existing_media["_id"]

    async def insert_media(
        self,
        metadata_info: dict,
        hash: str,
        channel: int,
        msg_id: int,
        size: str,
        name: str
    ) -> Optional[ObjectId]:
        encoded_string = await compact_encode(channel, msg_id, hash)

        if metadata_info['media_type'] == "movie":
            media = MovieSchema(
                tmdb_id=metadata_info['tmdb_id'],
                title=metadata_info['title'],
                genres=metadata_info['genres'],
                description=metadata_info['description'],
                rating=metadata_info['rate'],
                release_year=metadata_info['year'],
                poster=metadata_info['poster'],
                backdrop=metadata_info['backdrop'],
                runtime=metadata_info['runtime'],
                media_type=metadata_info['media_type'],
                languages=metadata_info['languages'],
                rip=metadata_info['rip'],
                views=0, # Initial views
                telegram=[
                    QualityDetail(
                        quality=metadata_info.get('quality') or 'Unknown',
                        id=encoded_string,
                        name=name,
                        size=size
                    )]
            )
            return await self.update_movie(media)
        else:
            tv_show = TVShowSchema(
                tmdb_id=metadata_info['tmdb_id'],
                title=metadata_info['title'],
                genres=metadata_info['genres'],
                description=metadata_info['description'],
                rating=metadata_info['rate'],
                release_year=metadata_info['year'],
                poster=metadata_info['poster'],
                backdrop=metadata_info['backdrop'],
                media_type=metadata_info['media_type'],
                status=metadata_info['status'],
                total_seasons=metadata_info['total_seasons'],
                total_episodes=metadata_info['total_episodes'],
                languages=metadata_info['languages'],
                rip=metadata_info['rip'],
                views=0, # Initial views
                seasons=[
                    Season(
                        season_number=metadata_info['season_number'],
                        episodes=[
                            Episode(
                                episode_number=metadata_info['episode_number'],
                                title=metadata_info['episode_title'],
                                episode_backdrop="",
                                telegram=[
                                    QualityDetail(
                                        quality=metadata_info.get('quality') or 'Unknown',
                                        id=encoded_string,
                                        name=name,
                                        size=size
                                    )
                                ]
                            )
                        ]
                    )
                ]
            )
            return await self.update_tv_show(tv_show)

    async def sort_tv_shows(
        self, 
        sort_params: List[Tuple[str, str]], 
        page: int, 
        page_size: int,
        genre: Optional[str] = None,
        year: Optional[str] = None,
        audio: Optional[str] = None,
        query: Optional[str] = None
    ) -> dict:
        skip = (page - 1) * page_size
        sort_criteria = [(field, ASCENDING if direction == "asc" else DESCENDING) 
                        for field, direction in sort_params]
        
        match_stage = {}
        if genre and genre.strip().lower() != "all":
            # Match if the genre array contains a case-insensitive version of the requested genre
            match_stage["genres"] = {"$regex": f"^{genre}$", "$options": "i"}
        if year and year.strip().lower() != "all":
            try:
                match_stage["release_year"] = int(year)
            except ValueError:
                pass
        if audio and audio.strip().lower() != "all":
            audio_low = audio.strip().lower()
            if audio_low in AUDIO_MAP:
                # Expand to multiple variations using regex OR
                variations = AUDIO_MAP[audio_low]
                pattern = f"^({'|'.join(variations)})$"
                match_stage["languages"] = {"$regex": pattern, "$options": "i"}
            else:
                match_stage["languages"] = {"$regex": f"^{audio}$", "$options": "i"}
        if query:
            match_stage["$or"] = [
                {"title": {"$regex": query, "$options": "i"}},
                {"slug": {"$regex": query, "$options": "i"}},
                {"tmdb_id": {"$regex": f"^{query}$"}} if isinstance(query, str) and query.isdigit() else {"tmdb_id": query}
            ]

        if not match_stage:
            total_count = await self.tv_collection.estimated_document_count()
        else:
            total_count = await self.tv_collection.count_documents(match_stage)
            
        # Project out the heavy 'seasons' field for listing
        cursor = self.tv_collection.find(match_stage, {"seasons": 0}).sort(sort_criteria).skip(skip).limit(page_size)
        data = await cursor.to_list(length=page_size)
        
        sorted_tv_shows = []
        settings = await self.get_settings()
        priority = settings.get("language_priority", [])
        
        for doc in data:
            try:
                doc = self._convert_object_id(doc)
                if "languages" in doc:
                    doc["languages"] = self._sort_languages(doc["languages"], priority)
                sorted_tv_shows.append(TVShowListSchema(**doc))
            except Exception as e:
                LOGGER.error(f"Validation error for TV show document {doc.get('_id')}: {e}")
                continue
                
        return {"total_count": total_count, "tv_shows": sorted_tv_shows}

    async def sort_movies(
        self, 
        sort_params: List[Tuple[str, str]], 
        page: int, 
        page_size: int,
        genre: Optional[str] = None,
        year: Optional[str] = None,
        audio: Optional[str] = None,
        query: Optional[str] = None
    ) -> dict:
        skip = (page - 1) * page_size
        sort_criteria = [(field, ASCENDING if direction == "asc" else DESCENDING) 
                        for field, direction in sort_params]
        
        match_stage = {}
        if genre and genre.strip().lower() != "all":
            match_stage["genres"] = {"$regex": f"^{genre}$", "$options": "i"}
        if year and year.strip().lower() != "all":
            try:
                match_stage["release_year"] = int(year)
            except ValueError:
                pass
        if audio and audio.strip().lower() != "all":
            audio_low = audio.strip().lower()
            if audio_low in AUDIO_MAP:
                # Expand to multiple variations using regex OR
                variations = AUDIO_MAP[audio_low]
                pattern = f"^({'|'.join(variations)})$"
                match_stage["languages"] = {"$regex": pattern, "$options": "i"}
            else:
                match_stage["languages"] = {"$regex": f"^{audio}$", "$options": "i"}
        if query:
            match_stage["$or"] = [
                {"title": {"$regex": query, "$options": "i"}},
                {"slug": {"$regex": query, "$options": "i"}},
                {"tmdb_id": {"$regex": f"^{query}$"}} if isinstance(query, str) and query.isdigit() else {"tmdb_id": query}
            ]

        if not match_stage:
            total_count = await self.movie_collection.estimated_document_count()
        else:
            total_count = await self.movie_collection.count_documents(match_stage)
            
        # Project out the heavy 'telegram' field for listing
        cursor = self.movie_collection.find(match_stage, {"telegram": 0, "external_links": 0}).sort(sort_criteria).skip(skip).limit(page_size)
        data = await cursor.to_list(length=page_size)
        
        sorted_movies = []
        settings = await self.get_settings()
        priority = settings.get("language_priority", [])
        
        for doc in data:
            try:
                doc = self._convert_object_id(doc)
                if "languages" in doc:
                    doc["languages"] = self._sort_languages(doc["languages"], priority)
                sorted_movies.append(MovieListSchema(**doc))
            except Exception as e:
                LOGGER.error(f"Validation error for movie document {doc.get('_id')}: {e}")
                continue
                
        return {"total_count": total_count, "movies": sorted_movies}

    async def get_trending_media(
        self,
        page: int = 1,
        page_size: int = 10
    ) -> dict:
        """
        Fetches the latest mixed media (Movies + TV Shows) sorted by updated_on descending.
        """
        skip = (page - 1) * page_size
        
        # Pipeline to get the latest from both collections
        # We increase the limit slightly to ensure we have enough for merging
        pipeline = [
            {"$sort": {"updated_on": DESCENDING}},
            {"$limit": skip + page_size * 2},
            {"$project": {"seasons": 0, "telegram": 0, "external_links": 0}}
        ]

        movie_results = await self.movie_collection.aggregate(pipeline).to_list(None)
        tv_results = await self.tv_collection.aggregate(pipeline).to_list(None)

        # Merge, sort by updated_on, and paginate
        combined = movie_results + tv_results
        
        # Robust sort handles cases where updated_on might be None or missing
        combined.sort(
            key=lambda x: x.get("updated_on") if isinstance(x.get("updated_on"), datetime) else datetime.min, 
            reverse=True
        )
        
        paginated_results = combined[skip:skip+page_size]

        # Total count should be correctly calculated or estimated
        total_movies = await self.movie_collection.estimated_document_count()
        total_tv = await self.tv_collection.estimated_document_count()

        # Ensure consistent data structure for frontend (rating, media_type, etc.)
        validated_results = []
        settings = await self.get_settings()
        priority = settings.get("language_priority", [])

        for doc in paginated_results:
            try:
                doc = self._convert_object_id(doc)
                if "languages" in doc:
                    doc["languages"] = self._sort_languages(doc["languages"], priority)
                
                # Coerce rating to float to avoid toFixed errors in frontend
                if "rating" in doc:
                    try:
                        doc["rating"] = float(doc["rating"])
                    except (TypeError, ValueError):
                        doc["rating"] = 0.0
                else:
                    doc["rating"] = 0.0

                if doc.get("media_type") == "movie":
                    validated_results.append(MovieListSchema(**doc))
                else:
                    validated_results.append(TVShowListSchema(**doc))
            except Exception as e:
                LOGGER.error(f"Trending validation error: {e}")
                continue

        return {"total_count": total_movies + total_tv, "results": validated_results}

    async def find_similar_media(
        self,
        tmdb_id: int,
        media_type: str,
        page: int = 1,
        page_size: int = 10
    ) -> dict:
        collection = self.movie_collection if media_type == "movie" else self.tv_collection
        parent_media = await collection.find_one({"tmdb_id": tmdb_id})
        
        if not parent_media:
            raise HTTPException(status_code=404, detail="Media not found")
        
        parent_genres = parent_media.get("genres", [])
        if not parent_genres:
            return {"total_count": 0, "similar_media": []}

        skip = (page - 1) * page_size
        pipeline = [
            {"$match": {
                "tmdb_id": {"$ne": tmdb_id},
                "genres": {"$in": parent_genres}
            }},
            {"$addFields": {
                "genreMatchCount": {"$size": {"$setIntersection": ["$genres", parent_genres]}}
            }},
            {"$sort": {"genreMatchCount": -1, "rating": -1}},
            {"$facet": {
                "metadata": [{"$count": "total_count"}],
                "data": [{"$skip": skip}, {"$limit": page_size}]
            }}
        ]
        
        result = await collection.aggregate(pipeline).to_list(1)
        total_count = result[0]["metadata"][0]["total_count"] if result[0]["metadata"] else 0
        
        settings = await self.get_settings()
        priority = settings.get("language_priority", [])
        
        similar_media = []
        for doc in result[0]["data"]:
            doc = self._convert_object_id(doc)
            if "languages" in doc:
                doc["languages"] = self._sort_languages(doc["languages"], priority)
            similar_media.append(doc)
            
        return {"total_count": total_count, "similar_media": similar_media}

    async def search_documents(
        self, 
        query: str, 
        page: int, 
        page_size: int
    ) -> dict:
        skip = (page - 1) * page_size
        words = query.split()
        regex_query = {'$regex': '.*' + '.*'.join(words) + '.*', '$options': 'i'}
        
        tv_pipeline = [
            {"$match": {"$or": [
                {"title": regex_query},
                {"seasons.episodes.telegram.name": regex_query}
            ]}},
            {"$project": {
                "_id": 1, "tmdb_id": 1, "title": 1, "slug": 1, "genres": 1, "rating": 1,
                "release_year": 1, "poster": 1, "backdrop": 1, "description": 1,
                "total_seasons": 1, "total_episodes": 1, "media_type": 1, "languages": 1,
                "rip": 1, "updated_on": 1
            }}
        ]
        
        movie_pipeline = [
            {"$match": {"$or": [
                {"title": regex_query},
                {"telegram.name": regex_query}
            ]}},
            {"$project": {
                "_id": 1, "tmdb_id": 1, "title": 1, "slug": 1, "genres": 1, "rating": 1,
                "release_year": 1, "poster": 1, "backdrop": 1, "description": 1,
                "media_type": 1, "languages": 1, "rip": 1, "updated_on": 1
            }}
        ]
        
        tv_results = await self.tv_collection.aggregate(tv_pipeline).to_list(None)
        movie_results = await self.movie_collection.aggregate(movie_pipeline).to_list(None)
        combined = tv_results + movie_results
        
        # Sort combined results by updated_on descending
        combined.sort(
            key=lambda x: x.get("updated_on") if isinstance(x.get("updated_on"), datetime) else datetime.min, 
            reverse=True
        )
        
        settings = await self.get_settings()
        priority = settings.get("language_priority", [])
        
        results = []
        for doc in combined[skip:skip+page_size]:
            doc = self._convert_object_id(doc)
            if "languages" in doc:
                doc["languages"] = self._sort_languages(doc["languages"], priority)
            results.append(doc)
            
        return {
            "total_count": len(combined),
            "results": results
        }

    async def get_media_details(
        self, 
        tmdb_id: Union[int, str], 
        season_number: Optional[int] = None, 
        episode_number: Optional[int] = None,
        title: Optional[str] = None,
        media_type: Optional[str] = None
    ) -> Union[dict, None]:
        # Unified search: Prioritize exact matches when both ID and Title are provided
        # This solves the "duplicate" issue where multiple entries have the same TMDB ID.
        
        main_query = {}
        if tmdb_id:
            main_query["tmdb_id"] = {"$in": [tmdb_id, str(tmdb_id)]}
        if title:
            # Match either the display title or the URL slug
            main_query["$or"] = [
                {"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}},
                {"slug": title}
            ]

        # If we have both, try exact match first
        if tmdb_id and title:
            queries_to_try = [
                main_query,
                {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}} # Fallback
            ]
        else:
            # Fallback to loose search if only one is provided
            query_parts = []
            if tmdb_id:
                query_parts.append({"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}})
                if isinstance(tmdb_id, str):
                    query_parts.append({"slug": tmdb_id})
                    query_parts.append({"title": tmdb_id})
            if title:
                query_parts.append({"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}})
                query_parts.append({"slug": title})
            
            queries_to_try = [{"$or": query_parts}] if query_parts else []

        if not queries_to_try:
            return None

        # Helper to find document in appropriate collection
        async def find_doc(q):
            if media_type == "tv":
                return await self.tv_collection.find_one(q), "tv"
            elif media_type == "movie":
                return await self.movie_collection.find_one(q), "movie"
            else:
                # Try TV then Movie
                doc = await self.tv_collection.find_one(q)
                if doc: return doc, "tv"
                doc = await self.movie_collection.find_one(q)
                if doc: return doc, "movie"
                return None, None

        doc = None
        detected_type = None
        for q in queries_to_try:
            doc, detected_type = await find_doc(q)
            if doc: break
        
        if not doc: return None

        # Episode/Season logic
        if episode_number is not None and season_number is not None:
            if detected_type != "tv": return None
            for season in doc.get("seasons", []):
                if str(season.get("season_number")) == str(season_number):
                    for episode in season.get("episodes", []):
                        if str(episode.get("episode_number")) == str(episode_number):
                            details = self._convert_object_id(episode)
                            details.update({
                                "tmdb_id": tmdb_id,
                                "type": "tv",
                                "season_number": season_number,
                                "episode_number": episode_number,
                                "backdrop": episode.get("episode_backdrop"),
                                "media_type": "tv" 
                            })
                            return details
            return None

        elif season_number is not None:
            if detected_type != "tv": return None
            for season in doc.get("seasons", []):
                if str(season.get("season_number")) == str(season_number):
                    episodes_list = season.get("episodes", [])
                    has_missing_data = False
                    for ep in episodes_list:
                        ep_bd = None
                        ep_title = ""
                        ep_num = None
                        if isinstance(ep, dict):
                            ep_bd = ep.get("episode_backdrop")
                            ep_title = ep.get("title", "")
                            ep_num = ep.get("episode_number")
                        elif hasattr(ep, "get"):
                            ep_bd = ep.get("episode_backdrop")
                            ep_title = ep.get("title", "")
                            ep_num = ep.get("episode_number")
                        else:
                            ep_bd = getattr(ep, "episode_backdrop", None)
                            ep_title = getattr(ep, "title", "")
                            ep_num = getattr(ep, "episode_number", None)
                        
                        if not ep_bd or is_generic_title(ep_title, ep_num):
                            has_missing_data = True
                            break

                    if has_missing_data:
                        try:
                            tmdb_id_val = doc.get("tmdb_id")
                            is_imdb = False
                            imdb_id = None
                            
                            if isinstance(tmdb_id_val, str):
                                if tmdb_id_val.startswith("tt"):
                                    is_imdb = True
                                    imdb_id = tmdb_id_val
                                elif tmdb_id_val.isdigit() and not Telegram.USE_TMDB:
                                    is_imdb = True
                                    imdb_id = f"tt{int(tmdb_id_val):07d}"
                            elif isinstance(tmdb_id_val, int) and not Telegram.USE_TMDB:
                                is_imdb = True
                                imdb_id = f"tt{tmdb_id_val:07d}"
                            
                            import httpx
                            
                            if is_imdb and Telegram.TMDB_API:
                                async with httpx.AsyncClient() as client:
                                    find_url = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={Telegram.TMDB_API}&external_source=imdb_id"
                                    find_res = await client.get(find_url)
                                    if find_res.status_code == 200:
                                        find_data = find_res.json()
                                        tv_results = find_data.get("tv_results", [])
                                        if tv_results:
                                            tmdb_id_val = tv_results[0]["id"]
                                            is_imdb = False
                            
                            changed = False
                            
                            async def fetch_tmdb_season(tv_id, season_num):
                                async with httpx.AsyncClient() as client:
                                    url = f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season_num}?api_key={Telegram.TMDB_API}"
                                    res = await client.get(url)
                                    if res.status_code == 200:
                                        return res.json()
                                    return None

                            async def fetch_tmdb_tv_details(tv_id):
                                async with httpx.AsyncClient() as client:
                                    url = f"https://api.themoviedb.org/3/tv/{tv_id}?api_key={Telegram.TMDB_API}"
                                    res = await client.get(url)
                                    if res.status_code == 200:
                                        return res.json()
                                    return None

                            if not is_imdb and Telegram.TMDB_API:
                                resolved_tmdb_id = int(tmdb_id_val) if isinstance(tmdb_id_val, (int, str)) and str(tmdb_id_val).isdigit() else tmdb_id_val
                                tmdb_tv_details = await fetch_tmdb_tv_details(resolved_tmdb_id)
                                
                                abs_to_rel = {}
                                if tmdb_tv_details and "seasons" in tmdb_tv_details:
                                    regular_seasons = sorted(
                                        [s for s in tmdb_tv_details["seasons"] if s.get("season_number", 0) > 0],
                                        key=lambda x: x.get("season_number")
                                    )
                                    current_abs = 1
                                    for s in regular_seasons:
                                        s_num = s.get("season_number")
                                        count = s.get("episode_count", 0)
                                        for r_num in range(1, count + 1):
                                            abs_to_rel[current_abs] = (s_num, r_num)
                                            current_abs += 1

                                fetched_seasons = {}
                                async def get_tmdb_season_data(s_num):
                                    if s_num not in fetched_seasons:
                                        fetched_seasons[s_num] = await fetch_tmdb_season(resolved_tmdb_id, s_num)
                                    return fetched_seasons[s_num]

                                # Loop over all episodes and perform healing
                                for ep in episodes_list:
                                    ep_num = None
                                    if isinstance(ep, dict):
                                        ep_num = ep.get("episode_number")
                                    else:
                                        ep_num = getattr(ep, "episode_number", None)
                                        
                                    if not ep_num:
                                        continue
                                        
                                    # Get episode count for the queried season to check if ep_num is relative
                                    current_season_info = next((s for s in tmdb_tv_details.get("seasons", []) if s.get("season_number") == season_number), None)
                                    current_season_ep_count = current_season_info.get("episode_count", 0) if current_season_info else 0

                                    # If the episode number is a valid relative number for the current season, use it directly.
                                    # Otherwise, attempt to map it as an absolute number.
                                    if 1 <= ep_num <= current_season_ep_count:
                                        target_s, target_rel_ep = season_number, ep_num
                                    elif ep_num in abs_to_rel:
                                        target_s, target_rel_ep = abs_to_rel[ep_num]
                                    else:
                                        target_s, target_rel_ep = season_number, ep_num
                                        
                                    tmdb_s_data = await get_tmdb_season_data(target_s)
                                    if tmdb_s_data:
                                        tmdb_episodes = {
                                            e['episode_number']: e
                                            for e in tmdb_s_data.get('episodes', [])
                                        }
                                        if target_rel_ep in tmdb_episodes:
                                            tmdb_info = tmdb_episodes[target_rel_ep]
                                            still_path = tmdb_info.get('still_path')
                                            name = tmdb_info.get('name')
                                            
                                            if isinstance(ep, dict):
                                                if still_path and not ep.get("episode_backdrop"):
                                                    ep["episode_backdrop"] = f"https://image.tmdb.org/t/p/original{still_path}"
                                                current_title = ep.get("title", "")
                                                if name and is_generic_title(current_title, ep_num):
                                                    ep["title"] = name
                                                    changed = True
                                            else:
                                                if still_path and not getattr(ep, "episode_backdrop", None):
                                                    setattr(ep, "episode_backdrop", f"https://image.tmdb.org/t/p/original{still_path}")
                                                current_title = getattr(ep, "title", "")
                                                if name and is_generic_title(current_title, ep_num):
                                                    setattr(ep, "title", name)
                                                    changed = True
                                                    
                                if not abs_to_rel or not tmdb_tv_details:
                                    # Fallback to direct season request if detail resolution failed
                                    tmdb_s_data = await fetch_tmdb_season(resolved_tmdb_id, season_number)
                                    if tmdb_s_data:
                                        tmdb_episodes = {
                                            e['episode_number']: e
                                            for e in tmdb_s_data.get('episodes', [])
                                        }
                                        for ep in episodes_list:
                                            ep_num = ep.get("episode_number") if isinstance(ep, dict) else getattr(ep, "episode_number", None)
                                            if ep_num in tmdb_episodes:
                                                tmdb_info = tmdb_episodes[ep_num]
                                                still_path = tmdb_info.get('still_path')
                                                name = tmdb_info.get('name')
                                                if isinstance(ep, dict):
                                                    if still_path and not ep.get("episode_backdrop"):
                                                        ep["episode_backdrop"] = f"https://image.tmdb.org/t/p/original{still_path}"
                                                    current_title = ep.get("title", "")
                                                    if name and is_generic_title(current_title, ep_num):
                                                        ep["title"] = name
                                                        changed = True
                                                else:
                                                    if still_path and not getattr(ep, "episode_backdrop", None):
                                                        setattr(ep, "episode_backdrop", f"https://image.tmdb.org/t/p/original{still_path}")
                                                    current_title = getattr(ep, "title", "")
                                                    if name and is_generic_title(current_title, ep_num):
                                                        setattr(ep, "title", name)
                                                        changed = True
                            elif is_imdb and Telegram.IMDB_API:
                                async with httpx.AsyncClient() as client:
                                    url = f"{Telegram.IMDB_API}/title/{imdb_id}/season/{season_number}"
                                    res = await client.get(url)
                                    if res.status_code == 200:
                                        imdb_season = res.json()
                                        imdb_episodes = {
                                            int(ep['no']): {
                                                'image': ep.get('image'),
                                                'title': ep.get('title')
                                            }
                                            for ep in imdb_season.get('episodes', [])
                                            if ep.get('no') and ep.get('no').isdigit()
                                        }
                                        
                                        for ep in episodes_list:
                                            if isinstance(ep, dict):
                                                ep_num = ep.get("episode_number")
                                                if ep_num in imdb_episodes:
                                                    imdb_info = imdb_episodes[ep_num]
                                                    if imdb_info.get('image') and not ep.get("episode_backdrop"):
                                                        ep["episode_backdrop"] = imdb_info['image']
                                                    current_title = ep.get("title", "")
                                                    if imdb_info.get('title') and is_generic_title(current_title, ep_num):
                                                        ep["title"] = imdb_info['title']
                                                        changed = True
                                            else:
                                                ep_num = getattr(ep, "episode_number", None)
                                                if ep_num in imdb_episodes:
                                                    imdb_info = imdb_episodes[ep_num]
                                                    if imdb_info.get('image') and not getattr(ep, "episode_backdrop", None):
                                                        setattr(ep, "episode_backdrop", imdb_info['image'])
                                                    current_title = getattr(ep, "title", "")
                                                    if imdb_info.get('title') and is_generic_title(current_title, ep_num):
                                                        setattr(ep, "title", imdb_info['title'])
                                                        changed = True
                            
                            if changed:
                                serializable_episodes = []
                                for ep in episodes_list:
                                    if isinstance(ep, dict):
                                        serializable_episodes.append(ep)
                                    elif hasattr(ep, "model_dump"):
                                        serializable_episodes.append(ep.model_dump())
                                    elif hasattr(ep, "dict"):
                                        serializable_episodes.append(ep.dict())
                                    else:
                                        serializable_episodes.append(ep.__dict__)
                                
                                # Sanitize backdrops for database update
                                clean_serializable = []
                                for ep in serializable_episodes:
                                    clean_ep = ep.copy()
                                    bd = clean_ep.get("episode_backdrop")
                                    if bd and not is_custom_backdrop(bd):
                                        clean_ep["episode_backdrop"] = ""
                                    clean_serializable.append(clean_ep)

                                await self.tv_collection.update_one(
                                    {"_id": doc["_id"], "seasons.season_number": season_number},
                                    {"$set": {"seasons.$.episodes": clean_serializable}}
                                )
                                season["episodes"] = episodes_list
                        except Exception as e:
                            LOGGER.error(f"Error self-healing episode backdrops: {e}")
                    
                    details = self._convert_object_id(season)
                    details.update({
                        "tmdb_id": tmdb_id,
                        "type": "tv",
                        "season_number": season_number,
                        "media_type": "tv"
                    })
                    return details
            return None

        else:
            # Top-level media
            # Self-healing slug
            if "slug" not in doc or not doc["slug"]:
                new_slug = slugify(doc["title"])
                coll = self.tv_collection if detected_type == "tv" else self.movie_collection
                await coll.update_one({"_id": doc["_id"]}, {"$set": {"slug": new_slug}})
                doc["slug"] = new_slug

            settings = await self.get_settings()
            priority = settings.get("language_priority", [])
            doc = self._convert_object_id(doc)
            if "languages" in doc:
                doc["languages"] = self._sort_languages(doc["languages"], priority)
            
            doc["type"] = detected_type
            doc["media_type"] = detected_type
            return doc

    async def get_quality_details(
        self,
        tmdb_id: int,
        quality: str,
        season: Optional[int] = None,
        episode: Optional[int] = None
    ) -> List[Dict[str, int]]:
        if season is None:
            # Movie case
            doc = await self.movie_collection.find_one(
                {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}},
                {"telegram": 1}
            )
            if not doc:
                return []
            return [
                {
                    "id": item["id"], 
                    "name": item["name"],
                    "quality": item.get("quality", "Unknown"),
                    "size": item.get("size", "0B")
                }
                for item in doc.get("telegram", [])
                if item["quality"] == quality
            ]
        else:
            # TV show case
            doc = await self.tv_collection.find_one(
                {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}},
                {"seasons": 1}
            )
            if not doc:
                return []
            
            results = []
            for s in doc.get("seasons", []):
                if s["season_number"] == season:
                    episodes = s.get("episodes", [])
                    
                    # Filter by specific episode if provided
                    if episode is not None:
                        episodes = [ep for ep in episodes if ep["episode_number"] == episode]
                    
                    for ep in episodes:
                        results.extend([
                            {
                                "id": t["id"], 
                                "name": t["name"],
                                "quality": t.get("quality", "Unknown"),
                                "size": t.get("size", "0B")
                            }
                            for t in ep.get("telegram", [])
                            if t["quality"] == quality
                        ])
            return results


    async def delete_document(
        self,
        media_type: str,
        tmdb_id: Union[int, str],
        title: Optional[str] = None,
        doc_id: Optional[str] = None
    ) -> bool:
        collection = self.movie_collection if media_type in ["mov", "movie"] else self.tv_collection
        
        # Prioritize deletion by unique MongoDB ID
        if doc_id:
            try:
                result = await collection.delete_one(self._id_filter(doc_id))
                if result.deleted_count > 0:
                    LOGGER.info(f"Deleted document by ID: {doc_id}")
                    return True
            except Exception as e:
                LOGGER.error(f"Error deleting by doc_id {doc_id}: {e}")

        # Fallback to query-based deletion
        query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}}
        if title:
            import re
            pattern = f"^{re.escape(title)}$"
            query["$or"] = [
                {"title": {"$regex": pattern, "$options": "i"}},
                {"slug": {"$regex": pattern, "$options": "i"}}
            ]
            
        result = await collection.delete_many(query)
        
        if result.deleted_count > 0:
            LOGGER.info(f"{media_type} deleted successfully ({result.deleted_count} docs).")
            return True
        LOGGER.info(f"No document found for {query}.")
        return False

    async def delete_quality_link(
        self,
        media_type: str,
        tmdb_id: Union[int, str],
        quality_id: str,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        title: Optional[str] = None,
        doc_id: Optional[str] = None
    ) -> bool:
        collection = self.movie_collection if media_type in ["mov", "movie"] else self.tv_collection
        
        # Build precise query
        query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}}
        if doc_id: query = {"_id": ObjectId(doc_id)}
        elif title: query["title"] = title

        LOGGER.info(f"Quality delete: type={media_type}, query={query}, quality_id={quality_id}")

        if media_type in ["mov", "movie"]:
            result = await self.movie_collection.update_one(
                query,
                {
                    "$pull": {"telegram": {"id": quality_id}},
                    "$set": {"updated_on": datetime.utcnow()}
                }
            )
            success = result.modified_count > 0
            LOGGER.info(f"Movie quality delete success: {success} (modified: {result.modified_count})")
            return success
        else:
            # TV show case
            # We need to find the specific episode and pull from its telegram array
            result = await self.tv_collection.update_one(
                {
                    **query,
                    "seasons.season_number": season,
                    "seasons.episodes.episode_number": episode
                },
                {
                    "$pull": {"seasons.$[s].episodes.$[e].telegram": {"id": quality_id}},
                    "$set": {"updated_on": datetime.utcnow()}
                },
                array_filters=[{"s.season_number": season}, {"e.episode_number": episode}]
            )
            success = result.modified_count > 0
            LOGGER.info(f"TV quality delete success: {success} (modified: {result.modified_count})")
            return success

    async def update_quality_link(
        self,
        media_type: str,
        tmdb_id: Union[int, str],
        quality_id: str,
        updated_data: dict,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        title: Optional[str] = None,
        doc_id: Optional[str] = None
    ) -> bool:
        # updated_data might contain: quality, name, size
        if media_type in ["mov", "movie"]:
            # Construct the $set object for the specific quality item
            set_query = {"updated_on": datetime.utcnow()}
            for key, value in updated_data.items():
                set_query[f"telegram.$.{key}"] = value

            filter_query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}, "telegram.id": quality_id}
            if doc_id:
                from bson import ObjectId
                filter_query = {"_id": ObjectId(doc_id), "telegram.id": quality_id}
            elif title:
                import re
                filter_query["$or"] = [
                    {"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}},
                    {"slug": {"$regex": f"^{re.escape(title)}$", "$options": "i"}}
                ]

            result = await self.movie_collection.update_one(
                filter_query,
                {"$set": set_query}
            )
            return result.matched_count > 0
        else:
            set_query = {"updated_on": datetime.utcnow()}
            for key, value in updated_data.items():
                set_query[f"seasons.$[s].episodes.$[e].telegram.$[t].{key}"] = value

            filter_query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}}
            if doc_id:
                from bson import ObjectId
                filter_query = {"_id": ObjectId(doc_id)}
            elif title:
                import re
                filter_query["$or"] = [
                    {"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}},
                    {"slug": {"$regex": f"^{re.escape(title)}$", "$options": "i"}}
                ]

            result = await self.tv_collection.update_one(
                filter_query,
                {"$set": set_query},
                array_filters=[
                    {"s.season_number": season},
                    {"e.episode_number": episode},
                    {"t.id": quality_id}
                ]
            )
            return result.matched_count > 0

    async def delete_season(self, tmdb_id: int, season_number: int, title: Optional[str] = None, doc_id: Optional[str] = None) -> bool:
        filter_query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}}
        if doc_id:
            from bson import ObjectId
            filter_query = {"_id": ObjectId(doc_id)}
        elif title:
            import re
            filter_query["$or"] = [
                {"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}},
                {"slug": {"$regex": f"^{re.escape(title)}$", "$options": "i"}}
            ]

        result = await self.tv_collection.update_one(
            filter_query,
            {
                "$pull": {"seasons": {"season_number": season_number}},
                "$set": {"updated_on": datetime.utcnow()}
            }
        )
        return result.matched_count > 0

    async def delete_episode(self, tmdb_id: int, season_number: int, episode_number: int, title: Optional[str] = None, doc_id: Optional[str] = None) -> bool:
        filter_query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}, "seasons.season_number": season_number}
        if doc_id:
            from bson import ObjectId
            filter_query = {"_id": ObjectId(doc_id), "seasons.season_number": season_number}
        elif title:
            import re
            filter_query["$or"] = [
                {"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}},
                {"slug": {"$regex": f"^{re.escape(title)}$", "$options": "i"}}
            ]

        result = await self.tv_collection.update_one(
            filter_query,
            {
                "$pull": {"seasons.$.episodes": {"episode_number": episode_number}},
                "$set": {"updated_on": datetime.utcnow()}
            }
        )
        return result.matched_count > 0

    async def update_episode(self, tmdb_id: int, season_number: int, episode_number: int, updated_data: dict, title: Optional[str] = None, doc_id: Optional[str] = None) -> bool:
        # updated_data: title, episode_backdrop
        set_query = {"updated_on": datetime.utcnow()}
        for key, value in updated_data.items():
            set_query[f"seasons.$[s].episodes.$[e].{key}"] = value

        filter_query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}}
        if doc_id:
            from bson import ObjectId
            filter_query = {"_id": ObjectId(doc_id)}
        elif title:
            import re
            filter_query["$or"] = [
                {"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}},
                {"slug": {"$regex": f"^{re.escape(title)}$", "$options": "i"}}
            ]

        result = await self.tv_collection.update_one(
            filter_query,
            {"$set": set_query},
            array_filters=[
                {"s.season_number": season_number},
                {"e.episode_number": episode_number}
            ]
        )
        return result.matched_count > 0

    async def get_unlinked_files(self, page: int = 1, page_size: int = 20, query: Optional[str] = None) -> dict:
        skip = (page - 1) * page_size
        
        match_stage = {"is_linked": {"$ne": True}}
        if query:
            match_stage["name"] = {"$regex": query, "$options": "i"}

        pipeline = [
            {"$match": match_stage},
            {"$sort": {"received_at": DESCENDING}},
            {"$facet": {
                "metadata": [{"$count": "total_count"}],
                "data": [{"$skip": skip}, {"$limit": page_size}]
            }}
        ]
        result = await self.manual_collection.aggregate(pipeline).to_list(1)
        total_count = result[0]["metadata"][0]["total_count"] if result[0]["metadata"] else 0
        files = [self._convert_object_id(doc) for doc in result[0]["data"]]
        return {"total_count": total_count, "files": files}

    async def insert_manual_file(self, file_data: dict) -> bool:
        """Insert a file received from AUTH_CHANNEL into manual_files collection if not exists."""
        # Use a combination of chat_id and msg_id as unique identifier
        existing = await self.manual_collection.find_one({
            "chat_id": file_data["chat_id"],
            "msg_id": file_data["msg_id"]
        })
        if not existing:
            file_data["is_linked"] = False
            file_data["received_at"] = datetime.utcnow()
            await self.manual_collection.insert_one(file_data)
            return True
        return False

    async def link_manual_file(self, file_id: str) -> bool:
        """Mark a manual file as linked."""
        result = await self.manual_collection.update_one(
            self._id_filter(file_id),
            {"$set": {"is_linked": True, "linked_at": datetime.utcnow()}}
        )
        return result.modified_count > 0

    async def delete_manual_file(self, file_id: str) -> bool:
        """Delete a manual file entry."""
        result = await self.manual_collection.delete_one(self._id_filter(file_id))
        return result.deleted_count > 0
    async def get_analytics(self) -> dict:
        """Fetch general library statistics and view analytics."""
        total_movies = await self.movie_collection.estimated_document_count()
        total_tv = await self.tv_collection.estimated_document_count()
        total_manual = await self.manual_collection.count_documents({"is_linked": {"$ne": True}})

        # Latest entries
        latest_movies = await self.movie_collection.find().sort("updated_on", DESCENDING).limit(5).to_list(None)
        latest_tv = await self.tv_collection.find().sort("updated_on", DESCENDING).limit(5).to_list(None)

        # View statistics
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)
        month_start = today_start - timedelta(days=30)
        
        today_views = await self.view_analytics.count_documents({"timestamp": {"$gte": today_start}})
        yesterday_views = await self.view_analytics.count_documents({"timestamp": {"$gte": yesterday_start, "$lt": today_start}})
        monthly_views = await self.view_analytics.count_documents({"timestamp": {"$gte": month_start}})
        total_views_all = await self.view_analytics.estimated_document_count()

        # Top Viewed
        top_movies = await self.movie_collection.find().sort("views", DESCENDING).limit(5).to_list(None)
        top_tv = await self.tv_collection.find().sort("views", DESCENDING).limit(5).to_list(None)

        return {
            "stats": {
                "movies": total_movies,
                "tv_shows": total_tv,
                "manual_files": total_manual,
                "total_content": total_movies + total_tv,
                "today_views": today_views,
                "yesterday_views": yesterday_views,
                "monthly_views": monthly_views,
                "total_views": total_views_all
            },
            "recent": {
                "movies": [MovieSchema(**self._convert_object_id(m)) for m in latest_movies],
                "tv_shows": [TVShowSchema(**self._convert_object_id(t)) for t in latest_tv]
            },
            "top_viewed": {
                "movies": [MovieSchema(**self._convert_object_id(m)) for m in top_movies],
                "tv_shows": [TVShowSchema(**self._convert_object_id(t)) for t in top_tv]
            }
        }

    async def get_view_graph_data(self, period: str = "week") -> List[dict]:
        """Fetch daily view counts for the given period."""
        from datetime import timedelta
        end_date = datetime.utcnow()
        if period == "yesterday":
            start_date = end_date - timedelta(days=2)
        elif period == "week":
            start_date = end_date - timedelta(days=7)
        elif period == "month":
            start_date = end_date - timedelta(days=30)
        elif period == "year":
            start_date = end_date - timedelta(days=365)
        else: # max
            start_date = datetime(2025, 1, 1) # Arbitrary start

        pipeline = [
            {"$match": {"timestamp": {"$gte": start_date}}},
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        results = await self.view_analytics.aggregate(pipeline).to_list(None)
        return [{"date": r["_id"], "views": r["count"]} for r in results]

    async def add_to_watchlist(self, data: dict) -> bool:
        """owner_id = Firebase UID (logged in) or 'guest:<device_id>' (not logged in)."""
        await self.watchlist.update_one(
            {
                "owner_id": data["owner_id"],
                "tmdb_id": int(data["tmdb_id"]),
                "media_type": data["media_type"],
            },
            {
                "$set": {
                    "title": data.get("title"),
                    "poster": data.get("poster"),
                    "backdrop": data.get("backdrop"),
                    "rating": data.get("rating"),
                    "release_year": data.get("release_year"),
                    "genres": data.get("genres"),
                    "added_on": datetime.utcnow(),
                }
            },
            upsert=True,
        )
        return True

    async def get_watchlist(self, owner_id: str, limit: int = 200) -> list:
        cursor = self.watchlist.find({"owner_id": owner_id}).sort("added_on", DESCENDING).limit(limit)
        items = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            items.append(doc)
        return items

    async def remove_from_watchlist(self, owner_id: str, tmdb_id: Union[int, str], media_type: str) -> bool:
        result = await self.watchlist.delete_one({
            "owner_id": owner_id,
            "tmdb_id": int(tmdb_id) if str(tmdb_id).isdigit() else tmdb_id,
            "media_type": media_type,
        })
        return result.deleted_count > 0

    async def add_or_update_review(self, data: dict) -> bool:
        """One review per device per title — resubmitting updates it (edit-in-place)."""
        existing = await self.reviews.find_one({
            "owner_id": data["owner_id"],
            "tmdb_id": int(data["tmdb_id"]),
            "media_type": data["media_type"],
        })
        await self.reviews.update_one(
            {
                "owner_id": data["owner_id"],
                "tmdb_id": int(data["tmdb_id"]),
                "media_type": data["media_type"],
            },
            {
                "$set": {
                    "reviewer_name": data.get("reviewer_name") or "Anonymous",
                    "rating": int(data["rating"]),
                    "review_text": data.get("review_text"),
                    "updated_on": datetime.utcnow(),
                },
                "$setOnInsert": {
                    "created_on": existing["created_on"] if existing else datetime.utcnow(),
                },
            },
            upsert=True,
        )
        return True

    async def get_reviews(self, tmdb_id: Union[int, str], media_type: str, limit: int = 50) -> dict:
        cursor = self.reviews.find({
            "tmdb_id": int(tmdb_id),
            "media_type": media_type,
        }).sort("created_on", DESCENDING).limit(limit)

        items = []
        total_rating = 0
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            items.append(doc)
            total_rating += doc.get("rating", 0)

        average = round(total_rating / len(items), 1) if items else 0
        return {"items": items, "average_rating": average, "total_reviews": len(items)}

    async def delete_review(self, owner_id: str, tmdb_id: Union[int, str], media_type: str) -> bool:
        result = await self.reviews.delete_one({
            "owner_id": owner_id,
            "tmdb_id": int(tmdb_id) if str(tmdb_id).isdigit() else tmdb_id,
            "media_type": media_type,
        })
        return result.deleted_count > 0

    async def admin_delete_review_by_id(self, review_id: str) -> bool:
        result = await self.reviews.delete_one({"_id": ObjectId(review_id)})
        return result.deleted_count > 0

    async def get_reviewed_titles(self) -> list:
        """Admin overview: every title that has reviews, with count + average, newest activity first."""
        pipeline = [
            {"$group": {
                "_id": {"tmdb_id": "$tmdb_id", "media_type": "$media_type"},
                "total_reviews": {"$sum": 1},
                "average_rating": {"$avg": "$rating"},
                "last_review_on": {"$max": "$created_on"},
            }},
            {"$sort": {"last_review_on": -1}},
        ]
        results = await self.reviews.aggregate(pipeline).to_list(None)

        titles = []
        for r in results:
            tmdb_id = r["_id"]["tmdb_id"]
            media_type = r["_id"]["media_type"]
            collection = self.movie_collection if media_type == "movie" else self.tv_collection
            doc = await collection.find_one({"tmdb_id": tmdb_id}, {"title": 1, "poster": 1})
            titles.append({
                "tmdb_id": tmdb_id,
                "media_type": media_type,
                "title": doc.get("title") if doc else f"Unknown (TMDB {tmdb_id})",
                "poster": doc.get("poster") if doc else None,
                "total_reviews": r["total_reviews"],
                "average_rating": round(r["average_rating"], 1),
                "last_review_on": r["last_review_on"],
            })
        return titles

    async def get_reviews_admin(self, tmdb_id: Union[int, str], media_type: str) -> list:
        """Same as get_reviews but keeps _id usable for admin delete-by-id."""
        cursor = self.reviews.find({
            "tmdb_id": int(tmdb_id),
            "media_type": media_type,
        }).sort("created_on", DESCENDING)
        items = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            items.append(doc)
        return items

    async def increment_view(self, media_type: str, tmdb_id: Union[int, str], identifier: str = None, title: str = None, doc_id: str = None) -> bool:
        """Increment view count and log to analytics (unique per ID per day)."""
        collection = self.movie_collection if media_type in ["mov", "movie"] else self.tv_collection
        
        # Build precise query
        query = {"tmdb_id": {"$in": [int(tmdb_id) if str(tmdb_id).isdigit() else tmdb_id, str(tmdb_id)]}}
        if doc_id: 
            query = {"_id": ObjectId(doc_id)}
        elif title: 
            query["$or"] = [
                {"title": {"$regex": f"^{re.escape(title)}$", "$options": "i"}},
                {"slug": title}
            ]

        # Unique check: Check if this identifier viewed this content today
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        if identifier:
            exists = await self.view_analytics.find_one({
                "tmdb_id": int(tmdb_id) if str(tmdb_id).isdigit() else tmdb_id,
                "identifier": identifier,
                "timestamp": {"$gte": today_start}
            })
            if exists:
                return False

        # Update total views
        result = await collection.update_one(
            query,
            {"$inc": {"views": 1}}
        )
        
        if result.matched_count > 0:
            # Log to time-series analytics
            await self.view_analytics.insert_one({
                "media_type": media_type,
                "tmdb_id": int(tmdb_id) if str(tmdb_id).isdigit() else tmdb_id,
                "identifier": identifier,
                "timestamp": datetime.utcnow()
            })
            return True
        return False

    async def update_media_details(self, media_type: str, tmdb_id: Union[int, str], updated_data: dict) -> bool:
        """Update core metadata for a movie or TV show using optional originalTitle for versioning."""
        collection = self.movie_collection if media_type in ["mov", "movie"] else self.tv_collection
        
        orig_title = updated_data.pop("originalTitle", None)
        if "_id" in updated_data: del updated_data["_id"]
        
        query = {"tmdb_id": {"$in": [tmdb_id, str(tmdb_id)]}}
        if orig_title:
            query["title"] = orig_title
            
        updated_data["updated_on"] = datetime.utcnow()
        result = await collection.update_one(
            query,
            {"$set": updated_data}
        )
        return result.matched_count > 0

    async def get_all_data(self) -> dict:
        """Fetch all documents from every collection for a complete backup."""
        # Dynamically fetch all collections to ensure 100% data coverage
        collection_names = await self.db.list_collection_names()
        
        backup_data = {}
        for coll_name in collection_names:
            if coll_name.startswith("system."):
                continue
                
            collection = self.db[coll_name]
            docs = await collection.find().to_list(None)
            backup_data[coll_name] = [self._convert_object_id(doc) for doc in docs]
            
        return backup_data

    async def restore_all_data(self, backup_data: dict) -> bool:
        """Clear and re-populate all collections dynamically from backup data."""
        try:
            for coll_name, docs in backup_data.items():
                if not docs and not isinstance(docs, list):
                    continue
                
                # Dynamically get the collection
                collection = self.db[coll_name]
                
                # Clear current collection for a fresh start
                await collection.delete_many({})
                
                if docs:
                    processed_docs = []
                    for doc in docs:
                        # Convert string _id back to ObjectId if it's a 24-char hex string
                        # EXCEPT for collections known to use custom string IDs
                        if coll_name not in ["settings", "admin_auth", "deploy_config"]:
                            if "_id" in doc and isinstance(doc["_id"], str) and len(doc["_id"]) == 24:
                                try:
                                    doc["_id"] = ObjectId(doc["_id"])
                                except:
                                    pass
                        processed_docs.append(doc)
                        
                    await collection.insert_many(processed_docs)
                
            return True
        except Exception as e:
            LOGGER.error(f"Error restoring from backup: {e}")
            return False

    async def add_bot_admin(self, user_id: int) -> bool:
        """Add a user to the bot_admins collection."""
        try:
            await self.db["bot_admins"].update_one(
                {"user_id": user_id},
                {"$set": {"user_id": user_id, "added_on": datetime.utcnow()}},
                upsert=True
            )
            return True
        except Exception as e:
            LOGGER.error(f"Error adding bot admin: {e}")
            return False

    async def remove_bot_admin(self, user_id: int) -> bool:
        """Remove a user from the bot_admins collection."""
        try:
            await self.db["bot_admins"].delete_one({"user_id": user_id})
            return True
        except Exception as e:
            LOGGER.error(f"Error removing bot admin: {e}")
            return False

    async def get_bot_admins(self) -> List[int]:
        """Get all user IDs from the bot_admins collection."""
        try:
            admins = await self.db["bot_admins"].find({}).to_list(None)
            return [a["user_id"] for a in admins]
        except Exception as e:
            LOGGER.error(f"Error fetching bot admins: {e}")
            return []

    async def is_bot_admin(self, user_id: int) -> bool:
        """Check if a user is an admin or owner."""
        if user_id == Telegram.OWNER_ID:
            return True
        admin = await self.db["bot_admins"].find_one({"user_id": user_id})
        return admin is not None
    async def create_collection(self, title: str, thumbnail: str) -> Optional[str]:
        slug = slugify(title)
        # Ensure unique slug if needed, but for simplicity we'll just use title
        doc = {
            "title": title, 
            "slug": slug,
            "thumbnail": thumbnail, 
            "items": [], 
            "updated_on": datetime.utcnow()
        }
        result = await self.collection_collection.insert_one(doc)
        return str(result.inserted_id)


    async def get_collections(self, page: int = 1, page_size: int = 20, sort_by: List[str] = ["updated_on:desc"], query: Optional[str] = None) -> dict:
        skip = (page - 1) * page_size
        
        filter_query = {}
        if query and query.strip():
            filter_query["title"] = {"$regex": query.strip(), "$options": "i"}
            
        sort_criteria = []
        for s in sort_by:
            parts = s.split(":")
            field = parts[0]
            direction = parts[1] if len(parts) > 1 else "desc"
            sort_criteria.append((field, DESCENDING if direction == "desc" else ASCENDING))
            
        if filter_query:
            total_count = await self.collection_collection.count_documents(filter_query)
        else:
            total_count = await self.collection_collection.estimated_document_count()
            
        collections = await self.collection_collection.find(filter_query).sort(sort_criteria).skip(skip).limit(page_size).to_list(None)
        
        processed_collections = []
        for c in collections:
            if "slug" not in c or not c["slug"]:
                new_slug = slugify(c["title"])
                await self.collection_collection.update_one({"_id": c["_id"]}, {"$set": {"slug": new_slug}})
                c["slug"] = new_slug
            processed_collections.append(self._convert_object_id(c))

        return {
            "total_count": total_count,
            "collections": processed_collections
        }

    async def get_collection(self, collection_id: str, sort_by: Optional[str] = None) -> Optional[dict]:
        # Support finding by ObjectId or Slug
        query = {}
        try:
            query["_id"] = ObjectId(collection_id)
        except:
            query["slug"] = collection_id
            
        c = await self.collection_collection.find_one(query)
        if not c:
            return None
            
        # Self-healing: Generate slug if missing
        if "slug" not in c or not c["slug"]:
            new_slug = slugify(c["title"])
            await self.collection_collection.update_one({"_id": c["_id"]}, {"$set": {"slug": new_slug}})
            c["slug"] = new_slug

        c = self._convert_object_id(c)
        
        items = c.get("items", [])
        if not items:
            c["populated_items"] = []
            return c
            
        # Group by type for efficient fetching
        # We now need to fetch specific duplicates if title is present
        items_to_fetch = []
        for i in items:
            items_to_fetch.append({
                "tmdb_id": int(i["tmdb_id"]),
                "media_type": i["media_type"],
                "title": i.get("title")
            })
        
        # Group by type for efficient bulk fetching
        movie_queries = []
        tv_queries = []
        for i in items:
            q = {"tmdb_id": {"$in": [int(i["tmdb_id"]), str(i["tmdb_id"])]}}
            if i.get("title"):
                q["title"] = i["title"]
            
            if i["media_type"] == "movie":
                movie_queries.append(q)
            else:
                tv_queries.append(q)

        # Execute bulk fetches
        movie_docs = []
        if movie_queries:
            movie_docs = await self.movie_collection.find({"$or": movie_queries}).to_list(None)
        
        tv_docs = []
        if tv_queries:
            tv_docs = await self.tv_collection.find({"$or": tv_queries}).to_list(None)

        # Map results for reconstruction
        populated_dict = {}
        settings = await self.get_settings()
        priority = settings.get("language_priority", [])

        for doc in movie_docs + tv_docs:
            doc = self._convert_object_id(doc)
            if "languages" in doc:
                doc["languages"] = self._sort_languages(doc["languages"], priority)
            
            # Use same key format as in reconstruction loop
            m_type = "movie" if doc in movie_docs else "tv"
            key = f"{m_type}_{doc['tmdb_id']}_{doc.get('title')}"
            populated_dict[key] = doc
            
        # Reconstruct in the original order (or reversed if recently added)
        populated_items = []
        for i in items:
            key = f"{i['media_type']}_{i['tmdb_id']}_{i.get('title')}"
            if key in populated_dict:
                populated_items.append(populated_dict[key])
            else:
                # Fallback for old items or type mismatch
                found = next((doc for doc in (movie_docs if i["media_type"] == "movie" else tv_docs) 
                            if str(doc["tmdb_id"]) == str(i["tmdb_id"])), None)
                if found:
                    populated_items.append(self._convert_object_id(found))
        
        # Sort in memory if requested
        if sort_by and sort_by != "custom":
            field, direction = sort_by.split(":") if ":" in sort_by else (sort_by, "desc")
            reverse = direction == "desc"
            
            if field == "rating":
                populated_items.sort(key=lambda x: x.get("rating", 0.0), reverse=reverse)
            elif field == "year":
                populated_items.sort(key=lambda x: x.get("release_year", 0), reverse=reverse)
            elif field == "updated":
                populated_items.sort(key=lambda x: x.get("updated_on") if isinstance(x.get("updated_on"), datetime) else datetime.min, reverse=reverse)
            elif field == "title":
                populated_items.sort(key=lambda x: x.get("title", "").lower(), reverse=reverse)

        c["populated_items"] = populated_items
        return c

    async def add_to_collection(self, collection_id: str, tmdb_id: int, media_type: str, title: Optional[str] = None) -> bool:
        item_data = {"tmdb_id": tmdb_id, "media_type": media_type}
        if title:
            item_data["title"] = title
            
        result = await self.collection_collection.update_one(
            {"_id": ObjectId(collection_id)},
            {"$addToSet": {"items": item_data}, "$set": {"updated_on": datetime.utcnow()}}
        )
        return result.modified_count > 0
        
    async def remove_from_collection(self, collection_id: str, tmdb_id: int, media_type: str, title: Optional[str] = None) -> bool:
        pull_query = {"tmdb_id": tmdb_id, "media_type": media_type}
        if title:
            pull_query["title"] = title
            
        result = await self.collection_collection.update_one(
            {"_id": ObjectId(collection_id)},
            {"$pull": {"items": pull_query}, "$set": {"updated_on": datetime.utcnow()}}
        )
        return result.modified_count > 0

    async def delete_collection(self, collection_id: str) -> bool:
        result = await self.collection_collection.delete_one({"_id": ObjectId(collection_id)})
        return result.deleted_count > 0

    async def update_collection(self, collection_id: str, title: str, thumbnail: str, items: Optional[List[dict]] = None) -> bool:
        slug = slugify(title)
        update_doc = {"title": title, "slug": slug, "thumbnail": thumbnail, "updated_on": datetime.utcnow()}
        if items is not None:
            update_doc["items"] = items
        result = await self.collection_collection.update_one(
            {"_id": ObjectId(collection_id)},
            {"$set": update_doc}
        )
        return result.matched_count > 0

    async def bulk_add_to_collection(self, collection_id: str, items: List[dict]) -> int:
        # items: List of {tmdb_id, media_type, title}
        added_count = 0
        for item in items:
            item_data = {"tmdb_id": int(item["tmdb_id"]), "media_type": item["media_type"]}
            if item.get("title"):
                item_data["title"] = item["title"]
            
            result = await self.collection_collection.update_one(
                {"_id": ObjectId(collection_id)},
                {"$addToSet": {"items": item_data}, "$set": {"updated_on": datetime.utcnow()}}
            )
            if result.modified_count > 0:
                added_count += 1
        return added_count
    async def get_admin_credentials(self):
        """Fetch the admin credentials document."""
        return await self.admin_collection.find_one({"_id": "admin_credentials"})

    async def set_admin_credentials(self, username, hashed_password):
        """Set or update the admin credentials."""
        await self.admin_collection.update_one(
            {"_id": "admin_credentials"},
            {"$set": {"username": username, "password": hashed_password, "updated_at": datetime.utcnow()}},
            upsert=True
        )
        return True
        
    async def find_detail_by_id(self, detail_id: str) -> Optional[dict]:
        """Search for a telegram quality detail by its unique ID across movies and tv shows."""
        # Search in movies
        movie = await self.movie_collection.find_one({"telegram.id": detail_id})
        if movie:
            for q in movie.get("telegram", []):
                if q["id"] == detail_id:
                    return q
                    
        # Search in TV shows (deeply nested)
        async for tv in self.tv_collection.find({"seasons.episodes.telegram.id": detail_id}):
            for season in tv.get("seasons", []):
                for episode in season.get("episodes", []):
                    for q in episode.get("telegram", []):
                        if q["id"] == detail_id:
                            return q
        return None

    # --- Home Sections DB APIs ---
    async def get_home_sections(self) -> List[dict]:
        sections = await self.home_sections.find().sort([("position", ASCENDING)]).to_list(None)
        return [self._convert_object_id(s) for s in sections]

    async def create_home_section(self, section_data: dict) -> str:
        max_sec = await self.home_sections.find_one(sort=[("position", -1)])
        max_pos = max_sec.get("position", 0) if max_sec else 0
        section_data["position"] = max_pos + 1
        result = await self.home_sections.insert_one(section_data)
        return str(result.inserted_id)

    async def update_home_section(self, section_id: str, section_data: dict) -> bool:
        if "_id" in section_data:
            del section_data["_id"]
        # Do not allow manually overwriting position in general update unless explicitly requested
        if "position" in section_data:
            del section_data["position"]
        res = await self.home_sections.update_one(
            {"_id": ObjectId(section_id)},
            {"$set": section_data}
        )
        return res.modified_count > 0

    async def delete_home_section(self, section_id: str) -> bool:
        sec = await self.home_sections.find_one({"_id": ObjectId(section_id)})
        if not sec:
            return False
        deleted_pos = sec.get("position", 0)
        res = await self.home_sections.delete_one({"_id": ObjectId(section_id)})
        if res.deleted_count > 0:
            await self.home_sections.update_many(
                {"position": {"$gt": deleted_pos}},
                {"$inc": {"position": -1}}
            )
            return True
        return False

    async def _populate_items(self, items: List[dict]) -> List[dict]:
        if not items:
            return []
        movie_queries = []
        tv_queries = []
        for i in items:
            q = {"tmdb_id": {"$in": [int(i["tmdb_id"]), str(i["tmdb_id"])]}}
            if i.get("title"):
                q["title"] = i["title"]
            if i["media_type"] == "movie":
                movie_queries.append(q)
            else:
                tv_queries.append(q)

        movie_docs = []
        if movie_queries:
            movie_docs = await self.movie_collection.find(
                {"$or": movie_queries},
                {"telegram": 0, "external_links": 0, "manual_stream_url": 0}
            ).to_list(None)
        
        tv_docs = []
        if tv_queries:
            tv_docs = await self.tv_collection.find(
                {"$or": tv_queries},
                {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}
            ).to_list(None)

        populated_dict = {}
        settings = await self.get_settings()
        priority = settings.get("language_priority", [])

        for doc in movie_docs + tv_docs:
            doc = self._convert_object_id(doc)
            if "languages" in doc:
                doc["languages"] = self._sort_languages(doc["languages"], priority)
            m_type = "movie" if doc in movie_docs else "tv"
            key = f"{m_type}_{doc['tmdb_id']}_{doc.get('title')}"
            populated_dict[key] = doc

        populated_items = []
        for i in items:
            key = f"{i['media_type']}_{i['tmdb_id']}_{i.get('title')}"
            if key in populated_dict:
                populated_items.append(populated_dict[key])
            else:
                found = next((doc for doc in (movie_docs if i["media_type"] == "movie" else tv_docs) 
                            if str(doc["tmdb_id"]) == str(i["tmdb_id"])), None)
                if found:
                    populated_items.append(self._convert_object_id(found))
        return populated_items

    async def get_populated_home_sections(self) -> List[dict]:
        sections = await self.home_sections.find({"enabled": True}).sort([("position", ASCENDING)]).to_list(None)
        
        # Get settings once
        settings = await self.get_settings()
        priority = settings.get("language_priority", [])
        
        async def populate_section(sec):
            sec = self._convert_object_id(sec)
            sec_type = sec.get("section_type")
            media_type = sec.get("media_type", "both")
            limit = sec.get("limit", 10)
            
            items = []
            if sec_type == "latest":
                if media_type == "movie":
                    res = await self.movie_collection.find({}, {"telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                    items = [self._convert_object_id(x) for x in res]
                elif media_type == "tv":
                    res = await self.tv_collection.find({}, {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                    items = [self._convert_object_id(x) for x in res]
                else: # both
                    movies, tvshows = await asyncio.gather(
                        self.movie_collection.find({}, {"telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("updated_on", DESCENDING)]).limit(limit).to_list(None),
                        self.tv_collection.find({}, {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("updated_on", DESCENDING)]).limit(limit).to_list(None)
                    )
                    combined = [self._convert_object_id(x) for x in movies] + [self._convert_object_id(x) for x in tvshows]
                    combined.sort(key=lambda x: x.get("updated_on") if x.get("updated_on") else datetime.min, reverse=True)
                    items = combined[:limit]
            elif sec_type == "trending":
                if media_type == "movie":
                    res = await self.movie_collection.find({}, {"telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("views", DESCENDING)]).limit(limit).to_list(None)
                    items = [self._convert_object_id(x) for x in res]
                elif media_type == "tv":
                    res = await self.tv_collection.find({}, {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("views", DESCENDING)]).limit(limit).to_list(None)
                    items = [self._convert_object_id(x) for x in res]
                else: # both
                    movies, tvshows = await asyncio.gather(
                        self.movie_collection.find({}, {"telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("views", DESCENDING)]).limit(limit).to_list(None),
                        self.tv_collection.find({}, {"seasons": 0, "telegram": 0, "external_links": 0, "manual_stream_url": 0}).sort([("views", DESCENDING)]).limit(limit).to_list(None)
                    )
                    combined = [self._convert_object_id(x) for x in movies] + [self._convert_object_id(x) for x in tvshows]
                    combined.sort(key=lambda x: x.get("views", 0), reverse=True)
                    items = combined[:limit]
            elif sec_type == "top_release":
                raw_items = sec.get("items", [])[:limit]
                items = await self._populate_items(raw_items)
            elif sec_type == "recently_watched":
                items = []

            for item in items:
                if "languages" in item:
                    item["languages"] = self._sort_languages(item["languages"], priority)

            sec["items"] = items
            return sec

        populated = await asyncio.gather(*(populate_section(sec) for sec in sections))
        return list(populated)
