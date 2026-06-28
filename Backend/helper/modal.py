# ─────────────────────────────────────────────────────────────────────────────
# Author  : ThiruXD
# GitHub  : https://github.com/ThiruXD
# Portfolio: https://thiruxd.is-a.dev
# ─────────────────────────────────────────────────────────────────────────────
from datetime import datetime
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional
from typing import List, Optional

class CollectionItem(BaseModel):
    tmdb_id: int = Field(..., description="TMDB ID of the media")
    media_type: str = Field(..., description="Type of media (movie or tv)")
    title: Optional[str] = Field(None, description="Title of the specific duplicate")

class CollectionSchema(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    title: str = Field(..., description="Title of the collection")
    thumbnail: str = Field(..., description="URL to the landscape thumbnail image")
    items: List[CollectionItem] = Field(default=[], description="List of items in the collection")
    updated_on: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of the last update")

class QualityDetail(BaseModel):
    quality: str = Field(..., description="Quality of the video (e.g., 1080p, 720p)")
    id: str = Field(..., description="Unique hash for the video")
    name: str = Field(..., description="Original Filename of telegram file")
    size: str = Field(..., description="Size of the File")

class ExternalLink(BaseModel):
    name: str = Field(..., description="Name of the service (GDrive, Mega, etc.)")
    url: str = Field(..., description="The direct URL")

class Episode(BaseModel):
    episode_number: int = Field(..., description="Episode number within the season")
    title: str = Field(default="Unknown Episode", description="Title of the episode")
    episode_backdrop: str = Field(default="", description="Backdrop of Episode")
    telegram: Optional[List[QualityDetail]] = Field(None, description="List of available quality details")
    external_links: Optional[List[ExternalLink]] = Field(None, description="External download/watch links")
    manual_stream_url: Optional[str] = Field(None, description="Manual stream URL for Pop player")

class Season(BaseModel):
    season_number: int = Field(..., description="Season number within the TV show")
    episodes: List[Episode] = Field(default=[], description="List of episodes in the season")

class TVShowSchema(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    tmdb_id: int = Field(..., description="The TMDB ID of the TV show")
    title: str = Field(..., description="Title of the TV show")
    slug: Optional[str] = Field(None, description="Unique URL-friendly slug")
    genres: List[str] = Field(default=[], description="List of genres associated with the TV show")
    languages: List[str] = Field(default=[], description="Audio languages available for the show")
    description: Optional[str] = Field(default="No description available.", description="Brief description of the TV show")
    rating: float = Field(default=0.0, description="Average rating of the TV show")
    release_year: Optional[int] = Field(None, description="Release year of the TV show")
    poster: Optional[str] = Field(None, description="URL to the poster image")
    backdrop: Optional[str] = Field(None, description="URL to the backdrop image")
    total_seasons: int = Field(default=0, description="Total Season of tv show")
    total_episodes: int = Field(default=0, description="Total Episode of tv show")
    media_type: str = Field(default="tv", description="Media Type of the file")
    status: str = Field(default="Unknown", description="Status update of tv show")
    updated_on: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of the last update")
    rip: str = Field(default="Unknown", description="Media rip of the file")
    views: int = Field(default=0, description="Total view count")
    seasons: List[Season] = Field(default=[], description="List of seasons in the TV show")



class MovieSchema(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    tmdb_id: int = Field(..., description="The TMDB ID of the Movie")
    title: str = Field(..., description="Title of the Movie")
    slug: Optional[str] = Field(None, description="Unique URL-friendly slug")
    genres: List[str] = Field(default=[], description="List of genres associated with the Movie")
    description: Optional[str] = Field(default="No description available.", description="Brief description of the Movie")
    rating: float = Field(default=0.0, description="Average rating of the Movie")
    release_year: Optional[int] = Field(None, description="Release year of the Movie")
    poster: Optional[str] = Field(None, description="URL to the poster image")
    backdrop: Optional[str] = Field(None, description="URL to the backdrop image")
    media_type: str = Field(default="movie", description="Media Type of the file")
    runtime: int = Field(default=0, description="runtime of the movie")
    updated_on: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of the last update")
    languages: List[str] = Field(default=[], description="List of languages associated with the Movie")
    rip: str = Field(default="Unknown", description="Media rip of the file")
    views: int = Field(default=0, description="Total view count")
    telegram: Optional[List[QualityDetail]] = Field(None, description="List of available quality details")
    external_links: Optional[List[ExternalLink]] = Field(None, description="External download/watch links")
    manual_stream_url: Optional[str] = Field(None, description="Manual stream URL for Pop player")

class SettingsSchema(BaseModel):
    buttonColor: Optional[str] = Field(None, description="Primary button color")
    buttonHoverColor: Optional[str] = Field(None, description="Hover color for buttons")
    defaultTheme: Optional[str] = Field(None, description="Default theme (light/dark/system)")
    structuralTheme: Optional[str] = Field(None, description="Global structural architecture theme")
    showThemeToggle: Optional[bool] = Field(None, description="Whether to show the theme toggle button")
    showAds: Optional[bool] = Field(False, description="Global toggle to show or hide ads")
    adFooter: Optional[str] = Field(None, description="HTML/JS snippet for Footer Ad")
    adSidebar: Optional[str] = Field(None, description="HTML/JS snippet for Sidebar Ad")
    adPopup: Optional[str] = Field(None, description="HTML/JS snippet for Popup/Interstitial Ad")
    adBanner: Optional[str] = Field(None, description="HTML/JS snippet for Top Wide Banner Ad")
    adInFeed: Optional[str] = Field(None, description="HTML/JS snippet for In-Feed Grid Ad")
    adPlayerBottom: Optional[str] = Field(None, description="HTML/JS snippet for Below Player Ad")
    adHomeTrending: Optional[str] = Field(None, description="HTML/JS snippet for Below Trending Slider")
    adHomeLatest: Optional[str] = Field(None, description="HTML/JS snippet for Below Latest Movies Section")
    siteName: Optional[str] = Field(None, description="Site name for display")
    telegramUrl: Optional[str] = Field(None, description="Telegram community URL")
    tgUsername: Optional[str] = Field(None, description="Telegram Bot Username (without @)")
    fsubChannel: Optional[str] = Field(None, description="Force subscribe channel ID or username")
    logChannel: Optional[str] = Field(None, description="Log channel/group for addition notifications")
    showViews: bool = Field(default=True, description="Toggle to show views on media posts")
    shortenerApiUrl: Optional[str] = Field(None, description="Shortener API URL")
    shortenerApiKey: Optional[str] = Field(None, description="Shortener API Key")
    adSocialBar: Optional[str] = Field(None, description="HTML/JS snippet for Social Bar Ad")
    adSmartlink: Optional[str] = Field(None, description="HTML/JS snippet for Smartlink Ad")
    language_priority: List[str] = Field(default=[], description="List of languages to prioritize (in order)")
    logoTextFont: Optional[str] = Field(None, description="Font Family Name for text logo")
    logoCustomFontUrl: Optional[str] = Field(None, description="Direct Google Fonts URL (optional)")
    logoImageUrl: Optional[str] = Field(None, description="Image URL for the logo")
    logoFontSize: Optional[int] = Field(24, description="Font size for the logo text in pixels")
    logoImageSize: Optional[int] = Field(40, description="Height for the logo image in pixels")
    maintenanceMode: bool = Field(default=False, description="Whether the site is in maintenance mode")
    maintenanceMessage: str = Field(default="Our website is currently undergoing scheduled maintenance. We'll be back shortly!", description="Message to show during maintenance")
    telegramCaption: Optional[str] = Field(None, description="Custom caption for telegram bot files")
    showPlayerButton: bool = Field(default=True, description="Toggle to show the player button")
    showTelegramButton: bool = Field(default=True, description="Toggle to show the Telegram button")
    showDownloadButton: bool = Field(default=True, description="Toggle to show the Download button")
    showExternalLinks: bool = Field(default=True, description="Toggle to show External Links")
    useShortenerPlayer: bool = Field(default=True, description="Toggle shortener for player button")
    useShortenerTelegram: bool = Field(default=True, description="Toggle shortener for Telegram button")
    useShortenerDownload: bool = Field(default=True, description="Toggle shortener for Download button")
    useShortenerExternal: bool = Field(default=True, description="Toggle shortener for External Links")
    showQuality: bool = Field(default=True, description="Toggle to show quality tags")
    showSize: bool = Field(default=True, description="Toggle to show file sizes")
    telegramCaption: Optional[str] = Field(None, description="Custom caption for telegram bot files")
    
class MovieListSchema(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    tmdb_id: int
    title: str
    slug: Optional[str] = None
    genres: List[str] = []
    description: Optional[str] = None
    rating: float = 0.0
    release_year: Optional[int] = None
    poster: Optional[str] = None
    backdrop: Optional[str] = None
    media_type: str = "movie"
    updated_on: datetime
    views: int = 0
    languages: List[str] = []
    rip: str = "Unknown"

class TVShowListSchema(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    tmdb_id: int
    title: str
    slug: Optional[str] = None
    genres: List[str] = []
    description: Optional[str] = None
    rating: float = 0.0
    release_year: Optional[int] = None
    poster: Optional[str] = None
    backdrop: Optional[str] = None
    media_type: str = "tv"
    updated_on: datetime
    views: int = 0
    languages: List[str] = []
    total_seasons: int = 0
    total_episodes: int = 0
    rip: str = "Unknown"

class HomeSectionSchema(BaseModel):
    title: str
    enabled: bool = True
    section_type: str  # trending, latest, top_release, recently_watched
    media_type: str = "both"  # movie, tv, both
    limit: int = 10
    layout: str = "slider"  # slider, grid
    items: List[CollectionItem] = []
    position: Optional[int] = None
